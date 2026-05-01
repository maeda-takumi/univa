<?php
$dbFiles = glob(__DIR__ . '/*.sqlite') ?: [];
$records = [];

foreach ($dbFiles as $file) {
    try {
        $pdo = new PDO('sqlite:' . $file, null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]);

        $table = null;
        foreach (['transactions', 'transaction_history'] as $candidate) {
            $check = $pdo->query("SELECT name FROM sqlite_master WHERE type='table' AND name='{$candidate}'");
            if ($check !== false && $check->fetch()) {
                $table = $candidate;
                break;
            }
        }

        if ($table === null) {
            continue;
        }

        $stmt = $pdo->query('SELECT created_on, status, payment_type, amount, metadata_name, cardholder_name, cardholder_email FROM ' . $table);

        while ($row = $stmt->fetch()) {
            $createdOn = trim((string)($row['created_on'] ?? ''));
            if ($createdOn === '') {
                continue;
            }

            $date = date('Y-m-d', strtotime($createdOn));
            $status = trim((string)($row['status'] ?? ''));
            $paymentType = trim((string)($row['payment_type'] ?? ''));
            $amount = is_numeric($row['amount'] ?? null) ? (int)$row['amount'] : 0;
            $name = trim((string)($row['metadata_name'] ?? ''));
            if ($name === '') {
                $name = trim((string)($row['cardholder_name'] ?? ''));
            }
            $email = mb_strtolower(trim((string)($row['cardholder_email'] ?? '')));

            $records[] = [
                'date' => $date,
                'status' => $status,
                'payment_type' => $paymentType,
                'amount' => $amount,
                'person_key' => $email !== '' ? $email : $name,
            ];
        }
    } catch (Throwable $e) {
        continue;
    }
}

function statusLabel(string $status): string
{
    return match ($status) {
        'successful' => '成功',
        'failed' => '失敗',
        'awaiting' => '処理待ち',
        '' => '未設定',
        default => $status,
    };
}

function paymentTypeLabel(string $paymentType): string
{
    return match ($paymentType) {
        'bank_transfer' => '振込',
        'card' => 'カード',
        '' => '未設定',
        default => $paymentType,
    };
}

$statuses = [];
$allBucket = [];

foreach ($records as $record) {
    $status = $record['status'] !== '' ? $record['status'] : '__empty__';
    $date = $record['date'];

    if (!isset($statuses[$status])) {
        $statuses[$status] = [];
    }
    if (!isset($statuses[$status][$date])) {
        $statuses[$status][$date] = [
            'total_amount' => 0,
            'people' => [],
            'payment_totals' => [],
        ];
    }

    if (!isset($allBucket[$date])) {
        $allBucket[$date] = [
            'total_amount' => 0,
            'people' => [],
            'payment_totals' => [],
        ];
    }

    $key = $record['person_key'];
    $paymentType = $record['payment_type'] !== '' ? $record['payment_type'] : '__empty__';

    $statuses[$status][$date]['total_amount'] += $record['amount'];
    $allBucket[$date]['total_amount'] += $record['amount'];

    if ($key !== '') {
        $statuses[$status][$date]['people'][$key] = true;
        $allBucket[$date]['people'][$key] = true;
    }

    $statuses[$status][$date]['payment_totals'][$paymentType] = ($statuses[$status][$date]['payment_totals'][$paymentType] ?? 0) + $record['amount'];
    $allBucket[$date]['payment_totals'][$paymentType] = ($allBucket[$date]['payment_totals'][$paymentType] ?? 0) + $record['amount'];
}

$tabs = ['all' => ['label' => 'すべて', 'data' => $allBucket]];
foreach ($statuses as $status => $data) {
    $tabs[$status] = [
        'label' => statusLabel($status === '__empty__' ? '' : $status),
        'data' => $data,
    ];
}

foreach ($tabs as &$tab) {
    krsort($tab['data']);
}
unset($tab);

include __DIR__ . '/header.php';
?>

<section class="panel">
  <div class="panel-head">
    <h2>日別集計ダッシュボード</h2>
  </div>

  <?php if (empty($records)): ?>
    <div class="empty-state">
      <p>集計対象のデータがありません。</p>
    </div>
  <?php else: ?>
    <div class="status-tabs" role="tablist" aria-label="状態別集計タブ">
      <?php $idx = 0; foreach ($tabs as $statusKey => $tab): ?>
        <button
          class="status-tab<?= $idx === 0 ? ' is-active' : ''; ?>"
          type="button"
          role="tab"
          data-tab-target="tab-<?= htmlspecialchars($statusKey, ENT_QUOTES, 'UTF-8'); ?>"
          aria-selected="<?= $idx === 0 ? 'true' : 'false'; ?>"
        >
          <?= htmlspecialchars($tab['label'], ENT_QUOTES, 'UTF-8'); ?>
        </button>
      <?php $idx++; endforeach; ?>
    </div>

    <?php $tabIdx = 0; foreach ($tabs as $statusKey => $tab): ?>
      <div class="tab-panel<?= $tabIdx === 0 ? ' is-active' : ''; ?>" id="tab-<?= htmlspecialchars($statusKey, ENT_QUOTES, 'UTF-8'); ?>" role="tabpanel">
        <div class="daily-card-grid">
          <?php foreach ($tab['data'] as $date => $summary): ?>
            <article class="daily-card">
              <h3><?= htmlspecialchars($date, ENT_QUOTES, 'UTF-8'); ?></h3>
              <dl>
                <div><dt>合計 amount</dt><dd><?= number_format($summary['total_amount']); ?></dd></div>
                <div><dt>人数</dt><dd><?= count($summary['people']); ?>人</dd></div>
                <?php foreach ($summary['payment_totals'] as $paymentType => $total): ?>
                  <div>
                    <dt><?= htmlspecialchars(paymentTypeLabel($paymentType === '__empty__' ? '' : $paymentType), ENT_QUOTES, 'UTF-8'); ?> 合計</dt>
                    <dd><?= number_format($total); ?></dd>
                  </div>
                <?php endforeach; ?>
              </dl>
            </article>
          <?php endforeach; ?>
        </div>
      </div>
    <?php $tabIdx++; endforeach; ?>
  <?php endif; ?>
</section>

<script>
document.addEventListener('DOMContentLoaded', function () {
  const tabs = document.querySelectorAll('.status-tab');
  const panels = document.querySelectorAll('.tab-panel');

  tabs.forEach(function (tab) {
    tab.addEventListener('click', function () {
      const target = tab.dataset.tabTarget;

      tabs.forEach(function (t) {
        t.classList.remove('is-active');
        t.setAttribute('aria-selected', 'false');
      });
      panels.forEach(function (panel) {
        panel.classList.remove('is-active');
      });

      tab.classList.add('is-active');
      tab.setAttribute('aria-selected', 'true');

      const panel = document.getElementById(target);
      if (panel) {
        panel.classList.add('is-active');
      }
    });
  });
});
</script>

<?php include __DIR__ . '/footer.php'; ?>
