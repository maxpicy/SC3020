import { useState, useEffect } from 'react';
import { analyzeQuery, fetchTables } from '../api';

export default function QueryPanel({ onResult, loading, setLoading, setError }) {
  const [query, setQuery] = useState('');
  const [tables, setTables] = useState([]);

  useEffect(() => {
    fetchTables()
      .then(data => setTables(data.tables || []))
      .catch(() => {});
  }, []);

  const handleSubmit = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const result = await analyzeQuery(query.trim());
      onResult(result);
    } catch (e) {
      setError(e.message);
      onResult(null);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      handleSubmit();
    }
  };

  return (
    <div className="query-panel">
      <h2>SQL Query</h2>
      {tables.length > 0 && (
        <div className="table-chips">
          <span className="chip-label">Tables:</span>
          {tables.map(t => (
            <span key={t} className="chip" onClick={() => setQuery(q => q + ' ' + t)}>
              {t}
            </span>
          ))}
        </div>
      )}
      <textarea
        value={query}
        onChange={e => setQuery(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Enter SQL query here... (Ctrl+Enter to submit)"
        rows={6}
        spellCheck={false}
      />
      <button onClick={handleSubmit} disabled={loading || !query.trim()}>
        {loading ? 'Analyzing...' : 'Analyze Query'}
      </button>
    </div>
  );
}
