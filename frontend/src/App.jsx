import { useState } from 'react';
import QueryPanel from './components/QueryPanel';
import SQLAnnotatedView from './components/SQLAnnotatedView';
import QEPTreeView from './components/QEPTreeView';
import AQPSection from './components/AQPSection';
import './App.css';

function App() {
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [hoveredIdx, setHoveredIdx] = useState(null);
  const [selectedIdx, setSelectedIdx] = useState(null);
  const [tooltipPos, setTooltipPos] = useState(null);

  const handleResult = (data) => {
    setResult(data);
    setHoveredIdx(null);
    setSelectedIdx(null);
    setTooltipPos(null);
  };

  const tooltipAnnotation = hoveredIdx !== null && result
    ? result.annotations[hoveredIdx]
    : null;

  return (
    <div className="app">
      <header className="app-header">
        <h1>SC3020 SQL Query Annotation Tool</h1>
      </header>

      <main className="app-main">
        <QueryPanel
          onResult={handleResult}
          loading={loading}
          setLoading={setLoading}
          setError={setError}
        />

        {error && <div className="error-box">{error}</div>}

        {result && (
          <div className="results-container">
            <SQLAnnotatedView
              query={result.original_query}
              annotations={result.annotations}
              hoveredIdx={hoveredIdx}
              setHoveredIdx={setHoveredIdx}
              selectedIdx={selectedIdx}
              setSelectedIdx={setSelectedIdx}
              tooltipAnnotation={tooltipAnnotation}
              tooltipPos={tooltipPos}
              setTooltipPos={setTooltipPos}
            />

            <QEPTreeView
              qep={result.qep}
              annotations={result.annotations}
              hoveredIdx={hoveredIdx}
              setHoveredIdx={setHoveredIdx}
              selectedIdx={selectedIdx}
              setSelectedIdx={setSelectedIdx}
            />

            <AQPSection
              qep={result.qep}
              aqps={result.aqps}
            />
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
