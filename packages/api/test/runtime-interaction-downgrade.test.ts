/**
 * Downgrade/recovery regression for F306 Phase C question fields.
 *
 * The pre-Phase-C host reads stored `request.questions[]` with a strict schema
 * that has no `multiSelect` / `allowUnanswered`. This suite pins the recovery
 * procedure: a frozen copy of the base schema rejects the new records before
 * the rewrite and accepts every pending/history record after it, while
 * unrelated records stay byte-identical and replay is idempotent.
 */

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { parseRuntimeInteractionRequest } from '@cat-cafe/shared';
import { normalizeRuntimeInteractionRecord } from '../src/domains/runtime-interaction/RuntimeInteractionRecordModel.js';
import {
  APPLY_CONFIRMATION,
  assertApplyConfirmation,
  inspectStoredRecord,
  parseCliArgs,
  runDowngrade,
} from '../src/scripts/prepare-runtime-interaction-downgrade.js';
import { legacyRuntimeQuestionRequestSchema } from '../src/scripts/runtime-interaction-downgrade/legacy-question-schema.js';

const KEY_PREFIX = 'cat-cafe:';
const ACTIVE_KEY = `${KEY_PREFIX}runtime-interaction:active`;
const PENDING_KEY = `${KEY_PREFIX}runtime-interaction:user:user-1:pending`;
const INVOCATION_KEY = `${KEY_PREFIX}runtime-interaction:invocation:inv-1:active`;

class FakeRedis {
  hashes = new Map<string, Map<string, string>>();
  sets = new Map<string, Set<string>>();
  zsets = new Map<string, Set<string>>();

  seedHash(key, fields) {
    this.hashes.set(key, new Map(Object.entries(fields)));
  }

  seedSet(key, members) {
    this.sets.set(key, new Set(members));
  }

  seedZset(key, members) {
    this.zsets.set(key, new Set(members));
  }

  async scan(_cursor, _matchToken, pattern) {
    const prefix = pattern.endsWith('*') ? pattern.slice(0, -1) : pattern;
    const keys = new Set([...this.hashes.keys(), ...this.sets.keys(), ...this.zsets.keys()]);
    return ['0', [...keys].filter((key) => key.startsWith(prefix))];
  }

  async type(key) {
    if (this.hashes.has(key)) return 'hash';
    if (this.sets.has(key)) return 'set';
    if (this.zsets.has(key)) return 'zset';
    return 'none';
  }

  async hgetall(key) {
    return Object.fromEntries(this.hashes.get(key) ?? []);
  }

  async hset(key, field, value) {
    if (!this.hashes.has(key)) this.hashes.set(key, new Map());
    this.hashes.get(key).set(field, value);
    return 1;
  }

  async hdel(key, field) {
    return this.hashes.get(key)?.delete(field) ? 1 : 0;
  }

  async zrem(key, member) {
    return this.zsets.get(key)?.delete(member) ? 1 : 0;
  }

  async srem(key, member) {
    return this.sets.get(key)?.delete(member) ? 1 : 0;
  }

  hash(key) {
    return this.hashes.get(key);
  }

  requestOf(key) {
    const raw = this.hash(key)?.get('request');
    return raw ? JSON.parse(raw) : null;
  }
}

function questionRequest(overrides = {}) {
  return {
    version: 1,
    interactionId: 'ix-pending',
    kind: 'question',
    owner: { userId: 'user-1', threadId: 'thread-1', catId: 'chrys', invocationId: 'inv-1' },
    provider: { providerId: 'chrys', method: '_chrys/request_input', requestId: 'ask-1' },
    createdAt: 10,
    title: 'iCode 需要你的回答',
    questions: [{ id: 'q1', header: '部署', question: '选择目标', multiSelect: true, allowUnanswered: true }],
    ...overrides,
  };
}

function record(request, overrides = {}) {
  return {
    request: JSON.stringify(request),
    status: 'pending',
    hostEpoch: 'host-1',
    updatedAt: '20',
    revision: '3',
    returnBinding: JSON.stringify({ kind: 'provider_waiter' }),
    ...overrides,
  };
}

function seededRedis() {
  const redis = new FakeRedis();
  redis.seedHash(`${KEY_PREFIX}runtime-interaction:ix-pending`, record(questionRequest(), { submission: '{}' }));
  redis.seedHash(
    `${KEY_PREFIX}runtime-interaction:ix-history`,
    record(questionRequest({ interactionId: 'ix-history' }), {
      status: 'answered',
      revision: '4',
      terminal: JSON.stringify({ status: 'answered', reasonCode: 'answered', settledAt: 30 }),
    }),
  );
  redis.seedHash(
    `${KEY_PREFIX}runtime-interaction:ix-clean`,
    record(
      questionRequest({
        interactionId: 'ix-clean',
        questions: [{ id: 'q1', header: '部署', question: '选择目标' }],
      }),
    ),
  );
  redis.seedHash(
    `${KEY_PREFIX}runtime-interaction:ix-approval`,
    record(
      {
        version: 1,
        interactionId: 'ix-approval',
        kind: 'approval',
        owner: { userId: 'user-1', threadId: 'thread-1', catId: 'chrys', invocationId: 'inv-1' },
        provider: { providerId: 'chrys', method: '_chrys/request_permission', requestId: 'ask-2' },
        createdAt: 10,
        title: '批准',
        decisions: [{ id: 'accept', label: '允许', outcome: 'accept' }],
      },
      { status: 'answered' },
    ),
  );
  redis.seedSet(ACTIVE_KEY, ['ix-pending', 'ix-clean']);
  redis.seedSet(INVOCATION_KEY, ['ix-pending']);
  redis.seedZset(PENDING_KEY, ['ix-pending', 'ix-clean']);
  return redis;
}

describe('runtime interaction downgrade recovery', () => {
  it('defaults to a zero-write dry-run and requires an explicit apply confirmation', () => {
    const defaults = parseCliArgs([], {});
    assert.equal(defaults.apply, false);
    assert.equal(defaults.keyPrefix, KEY_PREFIX);
    assert.equal(defaults.redisUrl, 'redis://127.0.0.1:6399');
    assert.equal(parseCliArgs(['--', '--dry-run'], {}).apply, false);
    assert.equal(parseCliArgs(['--key-prefix', 'test'], {}).keyPrefix, 'test:');
    assert.equal(parseCliArgs(['--now', '1234'], {}).now, 1234);
    assert.throws(() => parseCliArgs(['--now', '-1'], {}), /non-negative integer/);
    assert.throws(() => parseCliArgs(['--unknown'], {}), /Unknown argument/);
    assert.throws(() => assertApplyConfirmation({ ...defaults, apply: true }), /requires --confirm/);
    assert.doesNotThrow(() => assertApplyConfirmation({ ...defaults, apply: true, confirm: APPLY_CONFIRMATION }));
  });

  it('dry-run reports the plan without writing or settling anything', async () => {
    const redis = seededRedis();
    const summary = await runDowngrade(redis, { apply: false, keyPrefix: KEY_PREFIX, now: 1000 });

    assert.equal(summary.interactionHashes, 4);
    assert.equal(summary.questionRecords, 3);
    assert.equal(summary.rewritable, 2);
    assert.equal(summary.pendingSettled, 1);
    assert.equal(summary.rewritten, 0);
    assert.equal(summary.untouched, 2);
    assert.equal(summary.invalid, 0);
    assert.equal(redis.hash(`${KEY_PREFIX}runtime-interaction:ix-pending`).get('status'), 'pending');
    assert.match(redis.hash(`${KEY_PREFIX}runtime-interaction:ix-pending`).get('request'), /multiSelect/);
    assert.equal(redis.zsets.get(PENDING_KEY).has('ix-pending'), true);
    assert.equal(redis.sets.get(ACTIVE_KEY).has('ix-pending'), true);
  });

  it('settles pending cards and rewrites pending plus retained history into the base strict schema', async () => {
    const redis = seededRedis();
    const pendingKey = `${KEY_PREFIX}runtime-interaction:ix-pending`;
    const historyKey = `${KEY_PREFIX}runtime-interaction:ix-history`;
    const beforePending = redis.requestOf(pendingKey);
    const beforeHistory = redis.requestOf(historyKey);

    // The frozen base reader must reject the stored shape before recovery.
    assert.equal(legacyRuntimeQuestionRequestSchema.safeParse(beforePending).success, false);
    assert.equal(legacyRuntimeQuestionRequestSchema.safeParse(beforeHistory).success, false);

    const summary = await runDowngrade(redis, { apply: true, keyPrefix: KEY_PREFIX, now: 1000 });
    assert.equal(summary.rewritable, 2);
    assert.equal(summary.pendingSettled, 1);
    assert.equal(summary.rewritten, 2);
    assert.equal(summary.invalid, 0);

    const pending = redis.hash(pendingKey);
    assert.equal(pending.get('status'), 'invalidated');
    assert.equal(pending.get('revision'), '4');
    assert.equal(pending.get('updatedAt'), '1000');
    assert.equal(pending.has('submission'), false);
    assert.deepEqual(JSON.parse(pending.get('terminal')), {
      status: 'invalidated',
      reasonCode: 'host_restarted',
      settledAt: 1000,
    });
    assert.equal(redis.zsets.get(PENDING_KEY).has('ix-pending'), false);
    assert.equal(redis.sets.get(ACTIVE_KEY).has('ix-pending'), false);
    assert.equal(redis.sets.get(INVOCATION_KEY).has('ix-pending'), false);

    const history = redis.hash(historyKey);
    assert.equal(history.get('status'), 'answered');
    assert.equal(
      history.get('terminal'),
      JSON.stringify({ status: 'answered', reasonCode: 'answered', settledAt: 30 }),
      'terminal history must not be re-settled',
    );

    for (const key of [pendingKey, historyKey]) {
      const rewritten = redis.requestOf(key);
      assert.equal('multiSelect' in rewritten.questions[0], false);
      assert.equal('allowUnanswered' in rewritten.questions[0], false);
      assert.equal(legacyRuntimeQuestionRequestSchema.safeParse(rewritten).success, true);
      // The current reader and store normalizer stay backward compatible with
      // the rewritten shape, so a forward upgrade does not need another repair.
      parseRuntimeInteractionRequest(rewritten);
      const normalized = normalizeRuntimeInteractionRecord({
        request: rewritten,
        status: 'invalidated',
        hostEpoch: 'host-1',
        updatedAt: 1000,
      });
      assert.equal(normalized.request.kind, 'question');
    }

    // Unrelated records are byte-identical.
    assert.equal(
      redis.hash(`${KEY_PREFIX}runtime-interaction:ix-clean`).get('request'),
      JSON.stringify(
        questionRequest({
          interactionId: 'ix-clean',
          questions: [{ id: 'q1', header: '部署', question: '选择目标' }],
        }),
      ),
    );
    const approvalRequest = redis.hash(`${KEY_PREFIX}runtime-interaction:ix-approval`).get('request');
    assert.match(approvalRequest, /"kind":"approval"/);

    const replay = await runDowngrade(redis, { apply: true, keyPrefix: KEY_PREFIX, now: 2000 });
    assert.equal(replay.rewritable, 0);
    assert.equal(replay.pendingSettled, 0);
    assert.equal(replay.rewritten, 0);
  });

  it('refuses to rewrite a record the base strict reader would still reject', async () => {
    const redis = new FakeRedis();
    const key = `${KEY_PREFIX}runtime-interaction:ix-extra`;
    const request = questionRequest({
      interactionId: 'ix-extra',
      questions: [{ id: 'q1', header: '部署', question: '选择目标', multiSelect: true, futureField: { note: 'keep' } }],
    });
    redis.seedHash(key, record(request));
    redis.seedZset(PENDING_KEY, ['ix-extra']);
    const before = redis.hash(key).get('request');

    const summary = await runDowngrade(redis, { apply: true, keyPrefix: KEY_PREFIX, now: 1000 });
    assert.equal(summary.rewritten, 0);
    assert.equal(summary.invalid, 1);
    assert.match(summary.failures[0].reason, /futureField/);
    assert.equal(redis.hash(key).get('request'), before);
    assert.equal(redis.hash(key).get('status'), 'pending');
    assert.equal(redis.zsets.get(PENDING_KEY).has('ix-extra'), true);
  });

  it('rejects malformed requests and key/id mismatches without aborting the scan', async () => {
    const redis = new FakeRedis();
    redis.seedHash(`${KEY_PREFIX}runtime-interaction:ix-bad-json`, {
      request: '{not json',
      status: 'pending',
      hostEpoch: 'host-1',
      updatedAt: '1',
      revision: '1',
    });
    redis.seedHash(
      `${KEY_PREFIX}runtime-interaction:ix-renamed`,
      record(questionRequest({ interactionId: 'ix-other' })),
    );
    const summary = await runDowngrade(redis, { apply: true, keyPrefix: KEY_PREFIX, now: 1000 });
    assert.equal(summary.invalid, 2);
    assert.deepEqual(
      summary.failures.map((failure) => failure.reason),
      ['request is not valid JSON', 'detail key does not match interactionId'],
    );
  });

  it('inspects keys with a non-default prefix', () => {
    const action = inspectStoredRecord(
      'test:runtime-interaction:ix-prefixed',
      record(questionRequest({ interactionId: 'ix-prefixed' })),
      'test:',
    );
    assert.equal(action.kind, 'rewrite');
  });
});
