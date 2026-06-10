const KAIZEN_BASE_URL = 'https://seu-dominio.com';
const KAIZEN_TOKEN = 'TROQUE_ESTE_TOKEN';
const DRY_RUN = false;
const IS_PREVIEW = AdsApp.getExecutionInfo().isPreview();

function main() {
  const startedAt = nowSaoPaulo();
  let status = 'success';
  let message = '';
  let spendRows = 0;

  try {
    if (IS_PREVIEW) {
      Logger.log('Preview OK. Nenhuma alteracao aplicada.');
      return;
    }
    spendRows = postSpendSnapshot('TODAY');
  } catch (err) {
    status = 'error';
    message = String(err && err.message ? err.message : err);
    throw err;
  } finally {
    postRunLog({
      created_at: startedAt,
      customer_id: String(AdsApp.currentAccount().getCustomerId()).replace(/\D/g, ''),
      account: AdsApp.currentAccount().getName(),
      status: status,
      spend_rows: spendRows,
      message: message
    });
  }
}

function nowSaoPaulo() {
  return Utilities.formatDate(new Date(), 'America/Sao_Paulo', "yyyy-MM-dd'T'HH:mm:ssXXX");
}

function todayAccount() {
  return Utilities.formatDate(new Date(), AdsApp.currentAccount().getTimeZone(), 'yyyy-MM-dd');
}

function postSpendSnapshot(range) {
  const day = todayAccount();
  const rows = [];
  const account = AdsApp.currentAccount();
  const customerId = String(account.getCustomerId()).replace(/\D/g, '');
  const accountName = account.getName();
  const query = `
    SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type,
           metrics.cost_micros, metrics.clicks, metrics.impressions, metrics.conversions
    FROM campaign
    WHERE segments.date = '${day}' AND campaign.status != REMOVED
  `;
  const iterator = AdsApp.search(query);
  while (iterator.hasNext()) {
    const row = iterator.next();
    rows.push({
      day: day,
      account: accountName,
      customer_id: customerId,
      campaign_id: String(row.campaign.id),
      campaign: row.campaign.name,
      channel_type: String(row.campaign.advertisingChannelType || ''),
      attr_id: String(row.campaign.id),
      cost: Number(row.metrics.costMicros || 0) / 1000000,
      clicks: Number(row.metrics.clicks || 0),
      impressions: Number(row.metrics.impressions || 0),
      conversions: Number(row.metrics.conversions || 0),
      campaign_status: String(row.campaign.status || '')
    });
  }
  postJson('/api/google-ads-script/spend', {rows: rows}, 'Spend endpoint');
  return rows.length;
}

function postRunLog(payload) {
  try {
    postJson('/api/google-ads-script/log', payload, 'Log endpoint');
  } catch (err) {
    Logger.log('Log post failed: ' + err);
  }
}

function postJson(path, payload, label) {
  const url = KAIZEN_BASE_URL + path + '?token=' + encodeURIComponent(KAIZEN_TOKEN);
  const response = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  const code = response.getResponseCode();
  const body = response.getContentText();
  if (code < 200 || code >= 300) throw new Error(label + ' failed HTTP ' + code + ': ' + body);
  return JSON.parse(body);
}
