const API_BASE = '/api';

export async function analyzeQuery(query) {
  const res = await fetch(`${API_BASE}/analyze`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || 'Analysis failed');
  }
  return res.json();
}

export async function fetchTables() {
  const res = await fetch(`${API_BASE}/tables`);
  if (!res.ok) {
    throw new Error('Failed to fetch tables');
  }
  return res.json();
}
