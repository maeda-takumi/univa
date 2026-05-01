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

        $pdo->exec(
            'CREATE TABLE IF NOT EXISTS spreadsheet_imports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_row_id TEXT UNIQUE,
                payload_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )'
        );

        $columnCheck = $pdo->query("PRAGMA table_info({$table})");
        $hasDbIdColumn = false;
        if ($columnCheck !== false) {
            while ($column = $columnCheck->fetch()) {
                if (($column['name'] ?? '') === 'db_id') {
                    $hasDbIdColumn = true;
                    break;
                }
            }
        }
        if (!$hasDbIdColumn) {
            $pdo->exec('ALTER TABLE ' . $table . ' ADD COLUMN db_id TEXT');
        }

        $stmt = $pdo->query('SELECT created_on, status, payment_type, amount, metadata_name, cardholder_name, cardholder_email, db_id FROM ' . $table);

        while ($row = $stmt->fetch()) {
            $createdOn = trim((string)($row['created_on'] ?? ''));
            if ($createdOn === '') {
                continue;
            }


            $timestamp = strtotime($createdOn);
            if ($timestamp === false) {
                continue;
            }

            $date = date('Y-m-d', $timestamp);
            $month = date('Y-m', $timestamp);
            $status = trim((string)($row['status'] ?? ''));
            $paymentType = trim((string)($row['payment_type'] ?? ''));
            $amount = is_numeric($row['amount'] ?? null) ? (int)$row['amount'] : 0;
            $name = trim((string)($row['metadata_name'] ?? ''));
            if ($name === '') {
                $name = trim((string)($row['cardholder_name'] ?? ''));
            }
            $email = mb_strtolower(trim((string)($row['cardholder_email'] ?? '')));

            $dbId = trim((string)($row['db_id'] ?? ''));
            $records[] = [
                'date' => $date,
                'month' => $month,
                'status' => $status,
                'payment_type' => $paymentType,
                'amount' => $amount,
                'name' => $name,
                'email' => $email,
                'person_key' => $email !== '' ? $email : $name,
                'db_id' => $dbId,
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
$months = [];

foreach ($records as $record) {
    $status = $record['status'] !== '' ? $record['status'] : '__empty__';
    $date = $record['date'];
    $month = $record['month'];

    $months[$month] = true;

    if (!isset($statuses[$status])) {
        $statuses[$status] = [];
    }
    if (!isset($statuses[$status][$date])) {
        $statuses[$status][$date] = [
            'total_amount' => 0,
            'people' => [],
            'payment_totals' => [],
            'details' => [],
            'linked_count' => 0,
            'total_count' => 0,
        ];
    }

    if (!isset($allBucket[$date])) {
        $allBucket[$date] = [
            'total_amount' => 0,
            'people' => [],
            'payment_totals' => [],
            'details' => [],
            'linked_count' => 0,
            'total_count' => 0,
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

    // 入金先別の集計は「合計金額」ではなく「件数」をカウント
    $statuses[$status][$date]['payment_totals'][$paymentType] = ($statuses[$status][$date]['payment_totals'][$paymentType] ?? 0) + 1;
    $allBucket[$date]['payment_totals'][$paymentType] = ($allBucket[$date]['payment_totals'][$paymentType] ?? 0) + 1;

    $isLinked = $record['db_id'] !== '';
    $detail = [
        'name' => $record['name'] !== '' ? $record['name'] : '未設定',
        'email' => $record['email'] !== '' ? $record['email'] : '未設定',
        'amount' => $record['amount'],
        'payment_type' => paymentTypeLabel($record['payment_type']),
        'is_linked' => $isLinked,
    ];
    $statuses[$status][$date]['details'][] = $detail;
    $allBucket[$date]['details'][] = $detail;

    $statuses[$status][$date]['total_count']++;
    $allBucket[$date]['total_count']++;
    if ($isLinked) {
        $statuses[$status][$date]['linked_count']++;
        $allBucket[$date]['linked_count']++;
    }
}

$tabs = ['all' => ['label' => 'すべて', 'data' => $allBucket]];
foreach ($statuses as $status => $data) {
    $tabs[$status] = [
        'label' => statusLabel($status === '__empty__' ? '' : $status),
        'data' => $data,
    ];
}

$availableMonths = array_keys($months);
rsort($availableMonths);
$currentMonth = date('Y-m');
$defaultMonth = in_array($currentMonth, $availableMonths, true)
    ? $currentMonth
    : ($availableMonths[0] ?? '');

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
    <div class="month-selector-wrap">
      <label for="monthSelector">表示月</label>
      <select id="monthSelector" class="month-selector">
        <?php foreach ($availableMonths as $month): ?>
          <option value="<?= htmlspecialchars($month, ENT_QUOTES, 'UTF-8'); ?>"<?= $month === $defaultMonth ? ' selected' : ''; ?>>
            <?= htmlspecialchars($month, ENT_QUOTES, 'UTF-8'); ?>
          </option>
        <?php endforeach; ?>
      </select>
    </div>
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
            <article
              class="daily-card js-detail-card"
              data-month="<?= htmlspecialchars(substr($date, 0, 7), ENT_QUOTES, 'UTF-8'); ?>"
              data-date="<?= htmlspecialchars($date, ENT_QUOTES, 'UTF-8'); ?>"
              data-detail='<?= htmlspecialchars(json_encode($summary['details'], JSON_UNESCAPED_UNICODE), ENT_QUOTES, 'UTF-8'); ?>'
            >
              <div class="daily-card-head">
                <h3><?= htmlspecialchars($date, ENT_QUOTES, 'UTF-8'); ?></h3>
                <?php
                  $linkedCount = (int)($summary['linked_count'] ?? 0);
                  $totalCount = (int)($summary['total_count'] ?? 0);
                  $isAllLinked = $totalCount > 0 && $linkedCount === $totalCount;
                  $iconPath = $isAllLinked ? 'img/check.png' : 'img/batsu.png';
                ?>
                <div class="link-status <?= $isAllLinked ? 'is-linked' : 'is-unlinked'; ?>">
                  <img src="<?= htmlspecialchars($iconPath, ENT_QUOTES, 'UTF-8'); ?>" alt="<?= $isAllLinked ? '紐づけ完了' : '未紐づけあり'; ?>">
                  <small><?= $linkedCount; ?>/<?= $totalCount; ?></small>
                </div>
              </div>
              <dl>
                <div><dt>合計 amount</dt><dd><?= number_format($summary['total_amount']); ?></dd></div>
                <div><dt>人数</dt><dd><?= count($summary['people']); ?>人</dd></div>
                <?php foreach ($summary['payment_totals'] as $paymentType => $count): ?>
                  <div>
                    <dt><?= htmlspecialchars(paymentTypeLabel($paymentType === '__empty__' ? '' : $paymentType), ENT_QUOTES, 'UTF-8'); ?> 件数</dt>
                    <dd><?= number_format($count); ?></dd>
                  </div>
                <?php endforeach; ?>
              </dl>
            </article>
          <?php endforeach; ?>
        </div>
      </div>
    <?php $tabIdx++; endforeach; ?>

    <div id="detailPopup" class="detail-popup" aria-hidden="true" role="dialog" aria-labelledby="detailPopupTitle">
      <div class="detail-popup-overlay" data-close-detail></div>
      <section class="detail-popup-panel">
        <header class="detail-popup-head">
          <h3 id="detailPopupTitle">詳細一覧</h3>
          <button type="button" class="icon-btn" data-close-detail aria-label="閉じる">×</button>
        </header>
        <div class="detail-popup-content">
          <ul id="detailPopupList" class="detail-list"></ul>
        </div>
      </section>
    </div>
  <?php endif; ?>
</section>

<script>
document.addEventListener('DOMContentLoaded', function () {
  const tabs = document.querySelectorAll('.status-tab');
  const panels = document.querySelectorAll('.tab-panel');
  const monthSelector = document.getElementById('monthSelector');
  const detailPopup = document.getElementById('detailPopup');
  const detailPopupTitle = document.getElementById('detailPopupTitle');
  const detailPopupList = document.getElementById('detailPopupList');

  function filterByMonth(month) {
    document.querySelectorAll('.js-detail-card').forEach(function (card) {
      card.style.display = card.dataset.month === month ? '' : 'none';
    });
  }

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
  if (monthSelector) {
    filterByMonth(monthSelector.value);
    monthSelector.addEventListener('change', function () {
      filterByMonth(monthSelector.value);
    });
  }

  function closeDetailPopup() {
    if (!detailPopup) {
      return;
    }
    detailPopup.classList.remove('is-open');
    detailPopup.setAttribute('aria-hidden', 'true');
  }

  document.querySelectorAll('.js-detail-card').forEach(function (card) {
    card.addEventListener('click', function () {
      if (!detailPopup || !detailPopupList || !detailPopupTitle) {
        return;
      }

      const date = card.dataset.date || '';
      detailPopupTitle.textContent = date + ' の詳細一覧';
      detailPopupList.innerHTML = '';

      let detail = [];
      try {
        detail = JSON.parse(card.dataset.detail || '[]');
      } catch (error) {
        detail = [];
      }

      detail.forEach(function (item) {
        const li = document.createElement('li');
        li.className = 'detail-item';
        li.innerHTML =
          '<span><strong>氏名:</strong> ' + item.name + '</span>' +
          '<span><strong>メアド:</strong> ' + item.email + '</span>' +
          '<span><strong>金額:</strong> ' + Number(item.amount || 0).toLocaleString() + '</span>' +
          '<span><strong>入金方法:</strong> ' + item.payment_type + '</span>';
        detailPopupList.appendChild(li);
      });

      detailPopup.classList.add('is-open');
      detailPopup.setAttribute('aria-hidden', 'false');
    });
  });

  document.querySelectorAll('[data-close-detail]').forEach(function (button) {
    button.addEventListener('click', closeDetailPopup);
  });
});
</script>

<?php include __DIR__ . '/footer.php'; ?>
