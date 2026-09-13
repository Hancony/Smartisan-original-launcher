package com.smartisanos.launcher.quicksearch;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Handler;
import android.os.Looper;
import android.text.TextUtils;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Application-level history store matching pristine QuickSearch history semantics.
 * Persistent I/O is serialized off the main thread; UI only consumes snapshots.
 */
public final class SearchHistoryRepository {
    public static final int TYPE_QUERY = 0;
    public static final int TYPE_APPLICATION = 1;
    public static final int MAX_VISIBLE_ENTRIES = 20;

    private static final String TAG = "QS_HISTORY";
    private static final String PREFS = "quicksearch_original_history";
    private static final String KEY_ENTRIES = "entries_v1";
    private static volatile SearchHistoryRepository sInstance;

    public interface Listener {
        void onHistorySnapshotChanged(HistorySnapshot snapshot);
    }

    public static final class HistoryEntry {
        public final String content;
        public final String packageName;
        public final int type;
        public final long timestamp;

        HistoryEntry(String content, String packageName, int type, long timestamp) {
            this.content = content;
            this.packageName = packageName == null ? "" : packageName;
            this.type = type;
            this.timestamp = timestamp;
        }
    }

    public static final class HistorySnapshot {
        public final List<HistoryEntry> entries;
        public final long generation;
        public final boolean persistentLoadComplete;

        HistorySnapshot(List<HistoryEntry> entries, long generation, boolean loaded) {
            this.entries = Collections.unmodifiableList(
                    new ArrayList<HistoryEntry>(entries));
            this.generation = generation;
            this.persistentLoadComplete = loaded;
        }
    }

    private final Context context;
    private final Handler mainHandler = new Handler(Looper.getMainLooper());
    private final ExecutorService diskExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactory() {
                @Override public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "QuickSearchHistoryStore");
                    thread.setDaemon(true);
                    return thread;
                }
            });
    private final CopyOnWriteArrayList<Listener> listeners =
            new CopyOnWriteArrayList<Listener>();
    private volatile HistorySnapshot snapshot = new HistorySnapshot(
            Collections.<HistoryEntry>emptyList(), 0L, false);
    private boolean loadScheduled;

    private SearchHistoryRepository(Context context) {
        this.context = context.getApplicationContext();
    }

    public static SearchHistoryRepository get(Context context) {
        SearchHistoryRepository current = sInstance;
        if (current == null) {
            synchronized (SearchHistoryRepository.class) {
                current = sInstance;
                if (current == null) {
                    current = new SearchHistoryRepository(context);
                    sInstance = current;
                }
            }
        }
        current.ensureLoaded();
        return current;
    }

    public HistorySnapshot getCurrentSnapshot() {
        ensureLoaded();
        return snapshot;
    }

    public void addListener(Listener listener) {
        if (listener == null) return;
        listeners.addIfAbsent(listener);
        ensureLoaded();
    }

    public void removeListener(Listener listener) {
        if (listener != null) listeners.remove(listener);
    }

    public void recordQuery(String query) {
        record(query, "", TYPE_QUERY);
    }

    public void recordApplication(String label, String packageName) {
        if (TextUtils.isEmpty(packageName)) return;
        record(label, packageName, TYPE_APPLICATION);
    }

    public void clear() {
        diskExecutor.execute(new Runnable() {
            @Override public void run() {
                context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                        .edit().remove(KEY_ENTRIES).commit();
                publish(Collections.<HistoryEntry>emptyList(), true, "CLEAR");
            }
        });
    }

    private synchronized void ensureLoaded() {
        if (snapshot.persistentLoadComplete || loadScheduled) return;
        loadScheduled = true;
        diskExecutor.execute(new Runnable() {
            @Override public void run() {
                ArrayList<HistoryEntry> loaded = readPersistent();
                synchronized (SearchHistoryRepository.this) {
                    loadScheduled = false;
                }
                publish(loaded, true, "LOAD");
            }
        });
    }

    private void record(final String rawContent, final String packageName, final int type) {
        if (TextUtils.isEmpty(rawContent)) return;
        final String content = rawContent.trim();
        if (TextUtils.isEmpty(content)) return;
        diskExecutor.execute(new Runnable() {
            @Override public void run() {
                ArrayList<HistoryEntry> entries = snapshot.persistentLoadComplete
                        ? new ArrayList<HistoryEntry>(snapshot.entries) : readPersistent();
                HistoryEntry replacement = new HistoryEntry(content, packageName, type,
                        System.currentTimeMillis());
                for (int i = entries.size() - 1; i >= 0; i--) {
                    if (sameIdentity(replacement, entries.get(i))) entries.remove(i);
                }
                entries.add(0, replacement);
                while (entries.size() > MAX_VISIBLE_ENTRIES) {
                    entries.remove(entries.size() - 1);
                }
                writePersistent(entries);
                publish(entries, true, type == TYPE_APPLICATION ? "RECORD_APP" : "RECORD_QUERY");
            }
        });
    }

    private ArrayList<HistoryEntry> readPersistent() {
        ArrayList<HistoryEntry> result = new ArrayList<HistoryEntry>();
        try {
            SharedPreferences prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
            String encoded = prefs.getString(KEY_ENTRIES, "");
            if (encoded == null || encoded.length() == 0) return result;
            JSONArray array = new JSONArray(encoded);
            for (int i = 0; i < array.length() && result.size() < MAX_VISIBLE_ENTRIES; i++) {
                JSONObject item = array.optJSONObject(i);
                if (item == null) continue;
                String content = item.optString("content", "").trim();
                int type = item.optInt("type", TYPE_QUERY);
                if (content.length() == 0 || (type != TYPE_QUERY && type != TYPE_APPLICATION)) {
                    continue;
                }
                result.add(new HistoryEntry(content, item.optString("package", ""), type,
                        item.optLong("timestamp", 0L)));
            }
            ArrayList<HistoryEntry> collapsed = collapseDuplicates(result);
            if (collapsed.size() != result.size()) {
                writePersistent(collapsed);
                Log.i(TAG, "QS_HISTORY_DUPLICATES_REMOVED count="
                        + (result.size() - collapsed.size()));
            }
            result = collapsed;
        } catch (Throwable error) {
            Log.w(TAG, "QS_HISTORY_READ_FAILED", error);
        }
        return result;
    }

    /** Migrates history written before each visible search term had one canonical record. */
    private static ArrayList<HistoryEntry> collapseDuplicates(List<HistoryEntry> source) {
        ArrayList<HistoryEntry> result = new ArrayList<HistoryEntry>();
        for (HistoryEntry candidate : source) {
            int duplicateIndex = -1;
            for (int i = 0; i < result.size(); i++) {
                HistoryEntry existing = result.get(i);
                if (sameIdentity(candidate, existing)) {
                    duplicateIndex = i;
                    break;
                }
            }
            if (duplicateIndex < 0) {
                result.add(candidate);
            } else if (candidate.type == TYPE_APPLICATION
                    && result.get(duplicateIndex).type != TYPE_APPLICATION) {
                // Keep the more useful application entry at the newest matching position.
                result.set(duplicateIndex, candidate);
            }
        }
        return result;
    }

    private static boolean sameIdentity(HistoryEntry left, HistoryEntry right) {
        boolean samePackage = left.type == TYPE_APPLICATION
                && right.type == TYPE_APPLICATION
                && !TextUtils.isEmpty(left.packageName)
                && left.packageName.equals(right.packageName);
        return samePackage || left.content.equalsIgnoreCase(right.content);
    }

    private void writePersistent(List<HistoryEntry> entries) {
        try {
            JSONArray array = new JSONArray();
            for (HistoryEntry entry : entries) {
                JSONObject item = new JSONObject();
                item.put("content", entry.content);
                item.put("package", entry.packageName);
                item.put("type", entry.type);
                item.put("timestamp", entry.timestamp);
                array.put(item);
            }
            boolean committed = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                    .edit().putString(KEY_ENTRIES, array.toString()).commit();
            if (!committed) Log.w(TAG, "QS_HISTORY_WRITE_FAILED reason=commit_false");
        } catch (Throwable error) {
            Log.w(TAG, "QS_HISTORY_WRITE_FAILED", error);
        }
    }

    private void publish(List<HistoryEntry> entries, boolean loaded, String reason) {
        HistorySnapshot next = new HistorySnapshot(entries, snapshot.generation + 1L, loaded);
        snapshot = next;
        Log.i(TAG, "QS_HISTORY_SNAPSHOT reason=" + reason + " generation="
                + next.generation + " count=" + next.entries.size());
        final HistorySnapshot delivered = next;
        mainHandler.post(new Runnable() {
            @Override public void run() {
                for (Listener listener : listeners) {
                    try {
                        listener.onHistorySnapshotChanged(delivered);
                    } catch (Throwable error) {
                        Log.w(TAG, "QS_HISTORY_LISTENER_FAILED", error);
                    }
                }
            }
        });
    }
}
