<?php
declare(strict_types=1);

const DB_FILE = __DIR__ . '/data/payments.sqlite';

function h(?string $value): string
{
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function to_japanese_status(?string $status): ?string
{
    if ($status === null) {
        return null;
    }

    $normalized = strtolower(trim($status));
    $map = [
        'pending' => '処理待ち',
        'awaiting' => '処理待ち',
        'unverified' => '未確認',
        'verified' => '確認済み',
        'processing' => '処理中',
        'captured' => '売上確定',
        'succeeded' => '成功',
        'failed' => '失敗',
        'error' => 'エラー',
        'canceled' => 'キャンセル',
        'cancelled' => 'キャンセル',
        'refunded' => '返金済み',
        'chargeback' => 'チャージバック',
        'unknown' => '不明',
    ];

    return $map[$normalized] ?? $status;
}

function to_japanese_event_type(?string $eventType): ?string
{
    if ($eventType === null) {
        return null;
    }

    $normalized = strtolower(trim($eventType));
    $map = [
        'payment.created' => '決済作成',
        'payment.awaiting' => '処理待ち',
        'payment.unverified' => '未確認',
        'payment.verified' => '確認済み',
        'payment.updated' => '決済更新',
        'payment.succeeded' => '売上',
        'payment.failed' => '売上失敗',
        'payment.canceled' => '決済キャンセル',
        'payment.cancelled' => '決済キャンセル',
        'payment.refunded' => '返金完了',
        'token_updated' => 'トークン更新',
        'token_deleted' => 'トークン削除',
        'token_three_ds_updated' => '3Dセキュア更新',
        'token_created' => 'リカーリングトークン発行',
    ];

    return $map[$normalized] ?? $eventType;
}

$transactions = [];
$error = null;

try {
    $pdo = new PDO('sqlite:' . DB_FILE);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $transactionsStmt = $pdo->query(
        'SELECT transaction_id, amount, email, payer_name, current_status, first_seen_at, last_seen_at, last_event_at
         FROM transactions
         ORDER BY last_seen_at DESC, first_seen_at DESC'
    );
    $transactions = $transactionsStmt->fetchAll(PDO::FETCH_ASSOC);

    $eventsStmt = $pdo->prepare(
        'SELECT id, event_type, status, occurred_at, received_at, payload_json
         FROM webhook_events
         WHERE transaction_id = :transaction_id
         ORDER BY COALESCE(occurred_at, received_at, created_at) ASC, id ASC'
    );

    foreach ($transactions as &$transaction) {
        $eventsStmt->execute([':transaction_id' => $transaction['transaction_id']]);
        $transaction['events'] = $eventsStmt->fetchAll(PDO::FETCH_ASSOC);
    }
    unset($transaction);
} catch (Throwable $e) {
    $error = 'DBの読み込みに失敗しました。';
}

include __DIR__ . '/header.php';
?>

<section class="panel table-panel">
    <p class="eyebrow">Transaction Timeline</p>
    <h2 class="section-title">親データ一覧</h2>

    <?php if ($error !== null): ?>
        <p class="empty"><?= h($error) ?></p>
    <?php elseif (count($transactions) === 0): ?>
        <p class="empty">表示できるデータがありません。</p>
    <?php else: ?>
        <ul class="tx-list" aria-label="transactions">
            <?php foreach ($transactions as $tx): ?>
                <li class="tx-item">
                    <button class="tx-parent" type="button" aria-expanded="false">
                        <span class="tx-main">
                            <strong class="tx-name"><?= h($tx['payer_name'] ?? '-') ?></strong>
                            <small class="tx-email"><?= h($tx['email'] ?? '-') ?></small>
                            <small class="tx-id">ID: <?= h($tx['transaction_id']) ?></small>
                        </span>
                        <span class="tx-meta">
                            <small class="tx-event-date"><?= h($tx['last_event_at'] ?: $tx['last_seen_at']) ?></small>
                            <em class="status status-<?= h((string)$tx['current_status']) ?>"><?= h(to_japanese_status($tx['current_status']) ?? '不明') ?></em>
                            <b>¥<?= number_format((int)($tx['amount'] ?? 0)) ?></b>
                        </span>
                    </button>

                    <div class="tx-children" hidden>
                        <?php if (empty($tx['events'])): ?>
                            <p class="empty">イベント履歴はありません。</p>
                        <?php else: ?>
                            <ul>
                                <?php foreach ($tx['events'] as $ev): ?>
                                    <li class="event-item">
                                        <div>
                                            <strong><?= h(to_japanese_event_type($ev['event_type']) ?: '不明') ?></strong>
                                            <small><?= h(to_japanese_status($ev['status']) ?: '不明') ?></small>
                                        </div>
                                        <time><?= h($ev['occurred_at'] ?: $ev['received_at']) ?></time>
                                    </li>
                                <?php endforeach; ?>
                            </ul>
                        <?php endif; ?>
                    </div>
                </li>
            <?php endforeach; ?>
        </ul>
    <?php endif; ?>
</section>

<?php include __DIR__ . '/footer.php';
