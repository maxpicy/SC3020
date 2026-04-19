import { useState } from 'react';
import ConnectionPanel from './components/ConnectionPanel';
import QueryPanel from './components/QueryPanel';
import SQLAnnotatedView from './components/SQLAnnotatedView';
import QEPTreeView from './components/QEPTreeView';
import AQPSection from './components/AQPSection';
import './App.css';

function App() {
  const [connected, setConnected] = useState(false);
  // Bumped on every successful connect. Used as React `key` on QueryPanel so
  // it remounts and refetches its Tables chips, covering same-form-different-
  // backing-DB reconnects where the chip list needs to reflect a new schema.
  const [connectionKey, setConnectionKey] = useState(0);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [hoveredIdx, setHoveredIdx] = useState(null);
  const [selectedIdx, setSelectedIdx] = useState(null);
  const [tooltipPos, setTooltipPos] = useState(null);
  // Separate position for the pinned (clicked) tooltip so it persists after
  // the mouse leaves. Hover tooltipPos is transient; pinnedPos stays set until
  // the user unpins (clicks the same span/node again).
  const [pinnedPos, setPinnedPos] = useState(null);

  const handleResult = (data) => {
    setResult(data);
    setHoveredIdx(null);
    setSelectedIdx(null);
    setTooltipPos(null);
    setPinnedPos(null);
  };

  // Hover wins over pin so the user always sees the currently-pointed-at info,
  // but falling back to the pinned annotation means the tooltip doesn't vanish
  // on mouse-leave after a click.
  const tooltipAnnotation = result
    ? (hoveredIdx !== null
        ? result.annotations[hoveredIdx]
        : (selectedIdx !== null ? result.annotations[selectedIdx] : null))
    : null;
  const effectiveTooltipPos = hoveredIdx !== null ? tooltipPos : pinnedPos;

  return (
    <div className="app">
      <header className="app-header">
        <h1>SC3020 SQL Query Annotation Tool</h1>
      </header>

      <main className="app-main">
        <ConnectionPanel
          connected={connected}
          setConnected={setConnected}
          onConnected={() => {
            // Force a QueryPanel remount so its table-chip fetch re-fires,
            // covering same-config-but-different-backing-DB reconnects.
            setConnectionKey((k) => k + 1);
            setResult(null);
            setError(null);
          }}
        />

        {connected && (
          <QueryPanel
            key={connectionKey}
            onResult={handleResult}
            loading={loading}
            setLoading={setLoading}
            setError={setError}
          />
        )}

        {error && <div className="error-box">{error}</div>}

        {connected && result && (
          <div className="results-container">
            <SQLAnnotatedView
              query={result.original_query}
              annotations={result.annotations}
              hoveredIdx={hoveredIdx}
              setHoveredIdx={setHoveredIdx}
              selectedIdx={selectedIdx}
              setSelectedIdx={setSelectedIdx}
              tooltipAnnotation={tooltipAnnotation}
              tooltipPos={effectiveTooltipPos}
              setTooltipPos={setTooltipPos}
              setPinnedPos={setPinnedPos}
            />

            <QEPTreeView
              qep={result.qep}
              annotations={result.annotations}
              tableRowCounts={result.table_row_counts || {}}
              hoveredIdx={hoveredIdx}
              setHoveredIdx={setHoveredIdx}
              selectedIdx={selectedIdx}
              setSelectedIdx={setSelectedIdx}
              setPinnedPos={setPinnedPos}
            />

            <AQPSection
              qep={result.qep}
              aqps={result.aqps}
              qepOperators={result.qep_operators || []}
            />
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
