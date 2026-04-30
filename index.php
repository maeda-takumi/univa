<?php
declare(strict_types=1);

const DB_FILE = __DIR__ . '/data/payments.sqlite';

function h(?string $value): string
{
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

$transactions = [];
$error = null;

try {
    $pdo = new PDO('sqlite:' . DB_FILE);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $transactionsStmt = $pdo->query(
        'SELECT transaction_id, amount, email, payer_name, current_status, first_seen_at, last_seen_at
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
                        <span>
                            <strong><?= h($tx['transaction_id']) ?></strong>
                            <small><?= h($tx['email'] ?? '-') ?> / <?= h($tx['payer_name'] ?? '-') ?></small>
                        </span>
                        <span class="tx-meta">
                            <em class="status status-<?= h((string)$tx['current_status']) ?>"><?= h($tx['current_status']) ?></em>
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
                                            <strong><?= h($ev['event_type'] ?: 'unknown') ?></strong>
                                            <small><?= h($ev['status'] ?: 'unknown') ?></small>
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
