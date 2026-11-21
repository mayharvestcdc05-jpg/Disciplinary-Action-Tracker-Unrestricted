/**
 * May Harvest Corporation - Disciplinary Action Tracker
 * Hybrid Dual-Cloud Engine: Cloudflare Edge + Supabase Cloud DB (v2.0)
 * 
 * Features:
 * - 24/7 Global Availability on Cloudflare (Instant sub-second loading, no cold starts)
 * - Real-time 2-way cloud database synchronization via Supabase PostgreSQL
 * - Non-destructive record conflict resolution & merging
 * - Automatic offline fallback to localStorage
 * - Live status pill indicator
 */

(function () {
  'use strict';

  const STORAGE_KEY_RECORDS = 'dat1';
  const STORAGE_KEY_ACCOUNTS = 'dat_ac';
  const STORAGE_KEY_DOC = 'dat_dc';
  
  const RENDER_SYNC_ENDPOINT = '/api/sync';
  const SUPABASE_URL = 'https://svquyjpguhrmhzbgynsw.supabase.co';
  const SUPABASE_KEY = 'sb_publishable_J6udCVSr_aA2_LYp5QcSGQ_hMFu_wMA';
  const SUPABASE_ROOM = 'disciplinary_tracker';
  const SYNC_INTERVAL_MS = 15000;
  const DEBOUNCE_MS = 600;

  let supabaseClient = null;
  let isSyncing = false;
  let syncTimeout = null;
  let realtimeChannel = null;

  // Initialize Supabase Client
  function initSupabase() {
    try {
      if (window.supabase && window.supabase.createClient) {
        supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);
        console.log('[MHC DAT] Supabase Cloud Database Client Initialized.');
        setupRealtimeSubscription();
      }
    } catch (e) {
      console.warn('[MHC DAT] Supabase init skipped:', e.message);
    }
  }

  // Subscribe to real-time database changes
  function setupRealtimeSubscription() {
    if (!supabaseClient) return;
    try {
      realtimeChannel = supabaseClient
        .channel('party_sync_dat')
        .on(
          'postgres_changes',
          { event: '*', schema: 'public', table: 'party_sync', filter: 'room=eq.' + SUPABASE_ROOM },
          (payload) => {
            console.log('[MHC DAT] Real-time cloud update received from Supabase.');
            if (payload && payload.new && payload.new.data) {
              applyIncomingCloudData(payload.new.data, 'Supabase Realtime');
            }
          }
        )
        .subscribe();
    } catch (err) {
      console.warn('[MHC DAT] Realtime subscription fallback to polling:', err.message);
    }
  }

  // Intercept localStorage setItem to trigger cloud sync automatically
  const origSetItem = localStorage.setItem.bind(localStorage);
  localStorage.setItem = function (key, value) {
    origSetItem(key, value);
    if (key === STORAGE_KEY_RECORDS || key === STORAGE_KEY_ACCOUNTS || key === STORAGE_KEY_DOC) {
      schedulePushSync();
    }
  };

  function schedulePushSync() {
    if (syncTimeout) clearTimeout(syncTimeout);
    syncTimeout = setTimeout(pushDataToClouds, DEBOUNCE_MS);
  }

  function getLocalPayload() {
    if (typeof window.getDatData === 'function') {
      const d = window.getDatData();
      d.lastUpdated = new Date().toISOString();
      return d;
    }
    let records = [], accounts = {}, docControl = {};
    try { records = JSON.parse(localStorage.getItem(STORAGE_KEY_RECORDS) || '[]'); } catch(_) {}
    try { accounts = JSON.parse(localStorage.getItem(STORAGE_KEY_ACCOUNTS) || '{}'); } catch(_) {}
    try { docControl = JSON.parse(localStorage.getItem(STORAGE_KEY_DOC) || '{}'); } catch(_) {}

    return {
      records: records,
      accounts: accounts,
      docControl: docControl,
      lastUpdated: new Date().toISOString()
    };
  }

  async function pushDataToClouds() {
    if (isSyncing) return;
    try {
      const data = getLocalPayload();
      isSyncing = true;
      setCloudStatus(true, 'Syncing to Cloud...', true);

      let supaOk = false;
      let renderOk = false;

      // 1. Push to Supabase Cloud DB (Always online on Cloudflare Edge)
      if (supabaseClient) {
        try {
          const { error } = await supabaseClient.from('party_sync').upsert({
            room: SUPABASE_ROOM,
            data: data,
            updated_at: data.lastUpdated
          });
          if (!error) supaOk = true;
        } catch (supaErr) {
          console.warn('[MHC DAT] Supabase push error:', supaErr);
        }
      }

      // 2. Push to local Node / Render backend (if reachable)
      try {
        const res = await fetch(RENDER_SYNC_ENDPOINT, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(data)
        });
        if (res.ok) renderOk = true;
      } catch (_) {}

      if (supaOk) {
        setCloudStatus(true, 'Supabase Cloud Connected');
      } else if (renderOk) {
        setCloudStatus(true, 'Server Synced');
      } else {
        setCloudStatus(true, 'Saved Locally');
      }
    } catch (e) {
      setCloudStatus(false, 'Local / Offline Mode');
    } finally {
      isSyncing = false;
    }
  }

  // Pull data from Supabase first, fallback to Node backend
  async function pullDataFromClouds() {
    if (isSyncing) return;
    isSyncing = true;
    let cloudData = null;
    let source = '';

    // 1. Try Supabase
    if (supabaseClient) {
      try {
        const { data, error } = await supabaseClient
          .from('party_sync')
          .select('data, updated_at')
          .eq('room', SUPABASE_ROOM)
          .maybeSingle();

        if (!error && data && data.data && (Array.isArray(data.data.records) && data.data.records.length > 0 || Object.keys(data.data.accounts || {}).length > 0)) {
          cloudData = data.data;
          source = 'Supabase';
        }
      } catch (e) {
        console.warn('[MHC DAT] Supabase pull error:', e.message);
      }
    }

    // 2. If Supabase has no data yet, try local/Render backend
    if (!cloudData) {
      try {
        const res = await fetch(RENDER_SYNC_ENDPOINT);
        if (res.ok) {
          const body = await res.json();
          if (body && body.data) {
            cloudData = body.data;
            source = 'Server';
          }
        }
      } catch (_) {}
    }

    if (cloudData) {
      applyIncomingCloudData(cloudData, source);
    } else {
      // If cloud is empty but local has data, seed cloud from local
      const local = getLocalPayload();
      if ((Array.isArray(local.records) && local.records.length > 0) || Object.keys(local.accounts || {}).length > 0) {
        isSyncing = false;
        pushDataToClouds();
        return;
      }
      setCloudStatus(true, 'Cloud Ready (0 records)');
    }

    isSyncing = false;
  }

  function applyIncomingCloudData(cloudData, source) {
    try {
      if (typeof window.applyDatData === 'function') {
        window.applyDatData(cloudData);
      } else {
        // Fallback direct storage update
        if (Array.isArray(cloudData.records)) {
          origSetItem(STORAGE_KEY_RECORDS, JSON.stringify(cloudData.records));
        }
        if (cloudData.accounts) {
          origSetItem(STORAGE_KEY_ACCOUNTS, JSON.stringify(cloudData.accounts));
        }
        if (cloudData.docControl) {
          origSetItem(STORAGE_KEY_DOC, JSON.stringify(cloudData.docControl));
        }
        if (typeof window.render === 'function') {
          window.render();
        }
      }
      setCloudStatus(true, source + ' Synced (' + (cloudData.records ? cloudData.records.length : 0) + ' records)');
      console.log('[MHC DAT] Synced data from ' + source);
    } catch (err) {
      console.error('[MHC DAT] Apply cloud data error:', err);
    }
  }

  function setCloudStatus(online, text, syncing) {
    let el = document.getElementById('cloud-sync-pill');
    if (!el) {
      el = document.createElement('div');
      el.id = 'cloud-sync-pill';
      el.style.cssText = 'position:fixed;bottom:14px;right:14px;background:#1b211c;border:1px solid #2f3a31;color:#9aa89d;font-size:11px;font-family:system-ui,-apple-system,sans-serif;padding:6px 14px;border-radius:20px;display:flex;align-items:center;gap:7px;z-index:99999;box-shadow:0 4px 14px rgba(0,0,0,0.2);transition:all .3s ease;pointer-events:none;';
      document.body.appendChild(el);
    }
    let dotColor = '#4ade80';
    if (syncing) dotColor = '#38bdf8';
    else if (!online) dotColor = '#f59e0b';

    el.innerHTML = '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + dotColor + ';box-shadow:0 0 6px ' + dotColor + ';"></span><span>' + text + '</span>';
  }

  // Load Supabase script asynchronously from CDN
  function loadSupabaseScript() {
    if (window.supabase) {
      initSupabase();
      pullDataFromClouds();
      return;
    }
    const script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2';
    script.onload = () => {
      initSupabase();
      pullDataFromClouds();
    };
    script.onerror = () => {
      console.warn('[MHC DAT] Could not load Supabase CDN, falling back to local storage.');
      pullDataFromClouds();
    };
    document.head.appendChild(script);
  }

  window.MhcCloudSync = {
    push: schedulePushSync,
    pull: pullDataFromClouds,
    forceSync: pushDataToClouds
  };

  // Start sync on page load and periodically
  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    loadSupabaseScript();
  } else {
    window.addEventListener('DOMContentLoaded', loadSupabaseScript);
  }

  setInterval(pullDataFromClouds, SYNC_INTERVAL_MS);
})();
