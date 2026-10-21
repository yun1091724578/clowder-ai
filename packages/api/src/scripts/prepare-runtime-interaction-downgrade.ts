/**
 * Prepare persisted runtime interactions for a rollback to a revision whose
 * question schema predates `multiSelect` / `allowUnanswered`.
 *
 * Why: the previous strict reader rejects unknown question fields. Settling
 * pending cards alone is not enough — terminal/history records retain the
 * request JSON and become unreadable after a downgrade. This command rewrites
 * every stored question request into the frozen pre-Phase-C shape, and
 * invalidates cards that were still pending so the old host cannot show a
 * degraded (single-select / required-answer) version of them.
 *
 * Run with the API host stopped. Default is a zero-write dry-run.
 *
 *   pnpm --filter @cat-cafe/api run downgrade:runtime-interaction -- --redis-url <url>
 *   pnpm --filter @cat-cafe/api run downgrade:runtime-interaction -- --redis-url <url> \
 *     --apply --confirm "PREPARE RUNTIME INTERACTION DOWNGRADE"
 */

import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Redis } from 'ioredis';
import { legacyRuntimeQuestionRequestSchema } from './runtime-interaction-downgrade/legacy-question-schema.js';

const DEFAULT_REDIS_URL = 'redis://127.0.0.1:6399';
const DEFAULT_KEY_PREFIX = 'cat-cafe:';
const SCAN_COUNT = 200;

export const APPLY_CONFIRMATION = 'PREPARE RUNTIME INTERACTION DOWNGRADE';

export interface DowngradeCliArgs {
  redisUrl: string;
  keyPrefix: string;
  apply: boolean;
  confirm?: string;
  now: number;
}

export interface DowngradeRedis {
  scan(cursor: string, ...args: Array<string | number>): Promise<[string, string[]]>;
  type(key: string): Promise<string>;
  hgetall(key: string): Promise<Record<string, string>>;
  hset(key: string, field: string, value: string): Promise<number>;
  hdel(key: string, field: string): Promise<number>;
  zrem(key: string, member: string): Promise<number>;
  srem(key: string, member: string): Promise<number>;
}

export interface DowngradeFailure {
  key: string;
  interactionId: string | null;
  reason: string;
}

export interface DowngradeSummary {
  scanned: number;
  interactionHashes: number;
  questionRecords: number;
  rewritable: number;
  pendingSettled: number;
  rewritten: number;
  untouched: number;
  invalid: number;
  failures: DowngradeFailure[];
}

type RecordAction =
  | { kind: 'untouched'; reason: 'not_question' | 'no_new_fields' }
  | {
      kind: 'rewrite';
      key: string;
      interactionId: string;
      ownerUserId: string;
      invocationId: string;
      status: string;
      revision: number;
      request: Record<string, unknown>;
    }
  | { kind: 'invalid'; interactionId: string | null; reason: string };

function assignArgValue(
  args: DowngradeCliArgs,
  field: 'redisUrl' | 'keyPrefix' | 'confirm' | 'now',
  flag: string,
  value: string,
): void {
  if (field === 'now') {
    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed < 0) throw new Error(`${flag} requires a non-negative integer`);
    args.now = parsed;
    return;
  }
  args[field] = value;
}

export function parseCliArgs(argv: readonly string[], env: NodeJS.ProcessEnv = process.env): DowngradeCliArgs {
  const args: DowngradeCliArgs = {
    redisUrl: env.REDIS_URL ?? DEFAULT_REDIS_URL,
    keyPrefix: env.REDIS_KEY_PREFIX ?? DEFAULT_KEY_PREFIX,
    apply: false,
    now: Date.now(),
  };
  const valueFields = new Map<string, 'redisUrl' | 'keyPrefix' | 'confirm' | 'now'>([
    ['--redis-url', 'redisUrl'],
    ['--key-prefix', 'keyPrefix'],
    ['--confirm', 'confirm'],
    ['--now', 'now'],
  ]);
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--' || arg === '--dry-run') continue;
    if (arg === '--apply') {
      args.apply = true;
      continue;
    }
    const field = valueFields.get(arg);
    if (!field) throw new Error(`Unknown argument: ${arg}`);
    const value = argv[index + 1];
    if (!value || value.startsWith('--')) throw new Error(`${arg} requires a value`);
    assignArgValue(args, field, arg, value);
    index += 1;
  }
  args.keyPrefix = args.keyPrefix === '' || args.keyPrefix.endsWith(':') ? args.keyPrefix : `${args.keyPrefix}:`;
  return args;
}

export function assertApplyConfirmation(args: DowngradeCliArgs): void {
  if (args.apply && args.confirm !== APPLY_CONFIRMATION) {
    throw new Error(`--apply requires --confirm "${APPLY_CONFIRMATION}"`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function stripQuestionFields(request: Record<string, unknown>): {
  request: Record<string, unknown>;
  changed: boolean;
} {
  const questions = request.questions;
  if (!Array.isArray(questions)) return { request, changed: false };
  let changed = false;
  const stripped = questions.map((question) => {
    if (!isRecord(question)) return question;
    if (!Object.hasOwn(question, 'multiSelect') && !Object.hasOwn(question, 'allowUnanswered')) return question;
    changed = true;
    const next = { ...question };
    delete next.multiSelect;
    delete next.allowUnanswered;
    return next;
  });
  return changed ? { request: { ...request, questions: stripped }, changed } : { request, changed };
}

function describeIssues(error: { issues: Array<{ path: Array<string | number>; message: string }> }): string {
  const issue = error.issues[0];
  if (!issue) return 'legacy schema rejected the rewritten request';
  const path = issue.path.length > 0 ? issue.path.join('.') : '<root>';
  return `${path}: ${issue.message}`;
}

export function inspectStoredRecord(key: string, raw: Record<string, string>, keyPrefix: string): RecordAction {
  if (!raw.request) return { kind: 'invalid', interactionId: null, reason: 'missing request field' };
  let request: unknown;
  try {
    request = JSON.parse(raw.request);
  } catch {
    return { kind: 'invalid', interactionId: null, reason: 'request is not valid JSON' };
  }
  if (!isRecord(request)) return { kind: 'invalid', interactionId: null, reason: 'request is not an object' };
  if (request.kind !== 'question') return { kind: 'untouched', reason: 'not_question' };
  const interactionId = typeof request.interactionId === 'string' ? request.interactionId : null;
  if (!interactionId) return { kind: 'invalid', interactionId: null, reason: 'missing interactionId' };
  if (key !== `${keyPrefix}runtime-interaction:${interactionId}`) {
    return { kind: 'invalid', interactionId, reason: 'detail key does not match interactionId' };
  }
  const { request: rewritten, changed } = stripQuestionFields(request);
  if (!changed) return { kind: 'untouched', reason: 'no_new_fields' };

  const validation = legacyRuntimeQuestionRequestSchema.safeParse(rewritten);
  if (!validation.success) {
    return { kind: 'invalid', interactionId, reason: describeIssues(validation.error) };
  }
  const owner = rewritten.owner;
  if (!isRecord(owner) || typeof owner.userId !== 'string' || typeof owner.invocationId !== 'string') {
    return { kind: 'invalid', interactionId, reason: 'missing owner identity' };
  }
  const revision = Number(raw.revision ?? '1');
  return {
    kind: 'rewrite',
    key,
    interactionId,
    ownerUserId: owner.userId,
    invocationId: owner.invocationId,
    status: raw.status ?? 'unknown',
    revision: Number.isFinite(revision) && revision >= 1 ? revision : 1,
    // Write the stripped source, not the zod output: `nonBlank` trims, and a
    // downgrade must not rewrite human-authored text beyond removing the two
    // unsupported fields.
    request: rewritten,
  };
}

async function settlePendingCard(
  redis: DowngradeRedis,
  action: Extract<RecordAction, { kind: 'rewrite' }>,
  keyPrefix: string,
  now: number,
): Promise<void> {
  const terminal = { status: 'invalidated', reasonCode: 'host_restarted', settledAt: now };
  const detailKey = action.key;
  await redis.hset(detailKey, 'status', 'invalidated');
  await redis.hset(detailKey, 'terminal', JSON.stringify(terminal));
  await redis.hset(detailKey, 'updatedAt', String(now));
  await redis.hset(detailKey, 'revision', String(action.revision + 1));
  await redis.hdel(detailKey, 'submission');
  await redis.zrem(`${keyPrefix}runtime-interaction:user:${action.ownerUserId}:pending`, action.interactionId);
  await redis.srem(`${keyPrefix}runtime-interaction:active`, action.interactionId);
  await redis.srem(`${keyPrefix}runtime-interaction:invocation:${action.invocationId}:active`, action.interactionId);
}

async function handleInteractionRecord(
  redis: DowngradeRedis,
  key: string,
  args: Pick<DowngradeCliArgs, 'apply' | 'keyPrefix' | 'now'>,
  summary: DowngradeSummary,
): Promise<void> {
  const action = inspectStoredRecord(key, await redis.hgetall(key), args.keyPrefix);
  if (action.kind === 'invalid') {
    summary.invalid += 1;
    summary.failures.push({ key, interactionId: action.interactionId, reason: action.reason });
    return;
  }
  if (action.kind === 'untouched') {
    if (action.reason === 'no_new_fields') summary.questionRecords += 1;
    summary.untouched += 1;
    return;
  }
  summary.questionRecords += 1;
  summary.rewritable += 1;
  const settle = action.status === 'staged' || action.status === 'pending';
  if (settle) summary.pendingSettled += 1;
  if (!args.apply) return;
  if (settle) await settlePendingCard(redis, action, args.keyPrefix, args.now);
  // Rewrite last: a crash between settlement and rewrite leaves the fields in
  // place, so a rerun still detects and rewrites the record.
  await redis.hset(action.key, 'request', JSON.stringify(action.request));
  summary.rewritten += 1;
}

export async function runDowngrade(
  redis: DowngradeRedis,
  args: Pick<DowngradeCliArgs, 'apply' | 'keyPrefix' | 'now'>,
): Promise<DowngradeSummary> {
  const summary: DowngradeSummary = {
    scanned: 0,
    interactionHashes: 0,
    questionRecords: 0,
    rewritable: 0,
    pendingSettled: 0,
    rewritten: 0,
    untouched: 0,
    invalid: 0,
    failures: [],
  };
  let cursor = '0';
  do {
    const [nextCursor, keys] = await redis.scan(
      cursor,
      'MATCH',
      `${args.keyPrefix}runtime-interaction:*`,
      'COUNT',
      SCAN_COUNT,
    );
    cursor = nextCursor;
    for (const key of keys) {
      summary.scanned += 1;
      if ((await redis.type(key)) !== 'hash') continue;
      summary.interactionHashes += 1;
      await handleInteractionRecord(redis, key, args, summary);
    }
  } while (cursor !== '0');
  return summary;
}

function safeTargetLabel(redisUrl: string): string {
  const target = new URL(redisUrl);
  return `${target.hostname}:${target.port || '6379'}${target.pathname || '/'}`;
}

const CLI_USAGE = `Usage:
  pnpm --filter @cat-cafe/api downgrade:runtime-interaction -- [options]

Default mode is a zero-write dry-run. Stop the API host before --apply.

Options:
  --redis-url <url>      Redis target (default: REDIS_URL or redis://127.0.0.1:6399)
  --key-prefix <prefix>  Redis key prefix (default: REDIS_KEY_PREFIX or cat-cafe:)
  --now <ms>             Settlement timestamp for deterministic runs (default: now)
  --apply                Rewrite records and invalidate pending cards
  --confirm <text>       Required with --apply: "${APPLY_CONFIRMATION}"
  --help                 Show this message
`;

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  if (argv.includes('--help')) {
    process.stdout.write(CLI_USAGE);
    return;
  }
  const args = parseCliArgs(argv);
  assertApplyConfirmation(args);
  const redis = new Redis(args.redisUrl, { maxRetriesPerRequest: 3 });
  try {
    await redis.ping();
    const summary = await runDowngrade(redis as unknown as DowngradeRedis, args);
    process.stdout.write(
      `${JSON.stringify(
        { mode: args.apply ? 'apply' : 'dry-run', target: safeTargetLabel(args.redisUrl), ...summary },
        null,
        2,
      )}\n`,
    );
    if (summary.invalid > 0) process.exitCode = 1;
  } finally {
    await redis.quit();
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`[runtime-interaction-downgrade] ${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  });
}
