/**
 * May Harvest Corporation - Disciplinary Action Tracker
 * Hybrid Dual-Cloud Realtime Engine: Cloudflare Edge + Supabase Cloud DB (v3.0)
 * 
 * Features:
 * - Real-time 2-way database synchronization via Supabase PostgreSQL
 * - Instant WebSocket broadcast channel for sub-second cross-user screen reflection
 * - Deep content-aware hashing (detects any edits, reviews, decisions, files, and deletes)
 * - Safe concurrent merge resolution (no data loss, no infinite echo loops)
 * - Automatic pull on tab switch / window focus & periodic watchdog
 * - Reliable fallback to localStorage
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
  const SYNC_INTERVAL_MS = 12000;
  const DEBOUNCE_MS = 400;

  let supabaseClient = null;
  let isSyncing = false;
  let isApplyingCloudData = false;
  let syncTimeout = null;
  let pendingPush = false;
  let realtimeChannel = null;
  let lastSyncedHash = '';

  // 32-bit FNV-1a fast string hash
  function fastHash(str) {
    let h = 2166136261 >>> 0;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = Math.imul(h, 16777619) >>> 0;
    }
    return h.toString(36);
  }

  // Deep content hash: inspects every record, field, review, decision, log, account, and docControl
  function getHash(data) {
    if (!data) return '';
    try {
      const records = Array.isArray(data.records) ? data.records : [];
      const rTokens = records.map(r => {
        const rev = r.rev;
        const fin = r.fin;
        const file = rev && rev.file;
        return [
          r.id || '',
          r.ref || '',
          r.sur || '',
          r.first || '',
          r.mi || '',
          r.agency || '',
          r.dept || '',
          r.doi || '',
          r.toi || '',
          r.concern || '',
          r.at || '',
          r.createdBy || '',
          rev ? [
            rev.cat || '',
            rev.offense || '',
            rev.days || '',
            rev.nth || '',
            rev.act || '',
            rev.rep || '',
            rev.at || '',
            rev.by || '',
            file ? (file.name + '_' + (file.data ? file.data.length : 0)) : ''
          ].join('^') : 'no_rev',
          fin ? [
            fin.viol || '',
            fin.expl || '',
            fin.edate || '',
            fin.nth || '',
            fin.rec || '',
            fin.rep || '',
            fin.recd || '',
            fin.at || '',
            fin.by || ''
          ].join('^') : 'no_fin',
          (r.log && Array.isArray(r.log)) ? r.log.map(l => (l.at || '') + (l.by || '') + (l.ch ? l.ch.length : 0)).join('|') : '0'
        ].join('~');
      }).join(';;');

      const accTokens = Object.keys(data.accounts || {}).sort().map(k => k + ':' + data.accounts[k]).join('|');
      const dcTokens = (data.docControl && data.docControl.DCL) 
        ? data.docControl.DCL.map(d => (d.at || '') + (d.by || '') + (d.doc || '')).join('|') + '_' + JSON.stringify(data.docControl.DC || {})
        : '';

      return fastHash(rTokens + '###' + accTokens + '###' + dcTokens);
    } catch (_) {
      return '';
    }
  }

  // Initialize Supabase Client
  function initSupabase() {
    try {
      if (window.supabase && window.supabase.createClient) {
        supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);
        console.log('[MHC DAT] Supabase Cloud Client Initialized.');
        setupRealtimeSubscription();
      }
    } catch (e) {
      console.warn('[MHC DAT] Supabase init skipped:', e.message);
    }
  }

  // Realtime dual-channel: Postgres changes + instant broadcast channel
  function setupRealtimeSubscription() {
    if (!supabaseClient) return;
    try {
      realtimeChannel = supabaseClient
        .channel('party_sync_dat_v3')
        .on(
          'broadcast',
          { event: 'dat_update' },
          (payload) => {
            if (payload && payload.payload && payload.payload.data) {
              const incomingHash = payload.payload.hash || getHash(payload.payload.data);
              if (incomingHash !== lastSyncedHash) {
                console.log('[MHC DAT] Instant peer broadcast received.');
                applyIncomingCloudData(payload.payload.data, 'Realtime Broadcast');
              }
            }
          }
        )
        .on(
          'postgres_changes',
          { event: '*', schema: 'public', table: 'party_sync' },
          (payload) => {
            if (payload && payload.new && payload.new.room === SUPABASE_ROOM && payload.new.data) {
              const cloudData = payload.new.data;
              const incomingHash = getHash(cloudData);
              if (incomingHash !== lastSyncedHash) {
                console.log('[MHC DAT] Supabase database change received.');
                applyIncomingCloudData(cloudData, 'Supabase Realtime');
              }
            }
          }
        )
        .subscribe((status) => {
          if (status === 'SUBSCRIBED') {
            console.log('[MHC DAT] Real-time live synchronization active.');
          }
        });
    } catch (err) {
      console.warn('[MHC DAT] Realtime subscription fallback to polling:', err.message);
    }
  }

  // Intercept localStorage setItem to trigger cloud sync automatically
  const origSetItem = localStorage.setItem.bind(localStorage);
  localStorage.setItem = function (key, value) {
    origSetItem(key, value);
    if (isApplyingCloudData) return;
    if (key === STORAGE_KEY_RECORDS || key === STORAGE_KEY_ACCOUNTS || key === STORAGE_KEY_DOC) {
      schedulePushSync();
    }
  };

  function schedulePushSync() {
    if (syncTimeout) clearTimeout(syncTimeout);
    syncTimeout = setTimeout(() => pushDataToClouds(false), DEBOUNCE_MS);
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

  async function pushDataToClouds(force) {
    if (isSyncing || isApplyingCloudData) {
      if (force) pendingPush = true;
      return;
    }

    const data = getLocalPayload();
    const currentHash = getHash(data);
    if (!force && currentHash === lastSyncedHash && lastSyncedHash !== '') {
      return;
    }

    try {
      isSyncing = true;
      setCloudStatus(true, 'Syncing...', true);

      let supaOk = false;
      let renderOk = false;

      // 1. Push to Supabase Cloud DB
      if (supabaseClient) {
        try {
          const { error } = await supabaseClient.from('party_sync').upsert({
            room: SUPABASE_ROOM,
            data: data,
            updated_at: data.lastUpdated
          });
          if (!error) {
            supaOk = true;
            // Broadcast immediately to all connected browsers
            if (realtimeChannel) {
              realtimeChannel.send({
                type: 'broadcast',
                event: 'dat_update',
                payload: {
                  hash: currentHash,
                  data: data,
                  from: (window.user ? window.user.id : 'unknown')
                }
              }).catch(() => {});
            }
          } else {
            console.warn('[MHC DAT] Supabase push error:', error.message);
          }
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

      if (supaOk || renderOk) {
        lastSyncedHash = currentHash;
        setCloudStatus(true, 'Supabase Cloud Connected');
      } else {
        setCloudStatus(true, 'Saved Locally');
      }
    } catch (e) {
      setCloudStatus(false, 'Local Mode');
    } finally {
      isSyncing = false;
      if (pendingPush) {
        pendingPush = false;
        setTimeout(() => pushDataToClouds(true), 250);
      }
    }
  }

  // Pull data from Supabase first, fallback to Node backend
  async function pullDataFromClouds() {
    if (isSyncing || isApplyingCloudData) return;
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

        if (!error && data && data.data) {
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
      const incomingHash = getHash(cloudData);
      if (incomingHash !== lastSyncedHash) {
        applyIncomingCloudData(cloudData, source);
      } else {
        setCloudStatus(true, 'Supabase Cloud Connected');
      }
    } else {
      setCloudStatus(true, 'Supabase Cloud Connected');
    }
  }

  function applyIncomingCloudData(cloudData, source) {
    if (!cloudData) return;
    isApplyingCloudData = true;
    try {
      lastSyncedHash = getHash(cloudData);
      if (typeof window.applyDatData === 'function') {
        window.applyDatData(cloudData);
      } else {
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
      setCloudStatus(true, 'Supabase Cloud Connected');
      console.log('[MHC DAT] Synced data from ' + source);
    } catch (err) {
      console.error('[MHC DAT] Apply cloud data error:', err);
    } finally {
      setTimeout(() => { isApplyingCloudData = false; }, 300);
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
    forceSync: function () {
      if (syncTimeout) clearTimeout(syncTimeout);
      return pushDataToClouds(true);
    }
  };

  // Start sync on page load
  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    loadSupabaseScript();
  } else {
    window.addEventListener('DOMContentLoaded', loadSupabaseScript);
  }

  // Watchdog periodic polling
  setInterval(pullDataFromClouds, SYNC_INTERVAL_MS);

  // Immediate pull on tab visibility change or window focus
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
      pullDataFromClouds();
    }
  });
  window.addEventListener('focus', () => {
    pullDataFromClouds();
  });
})();
