import { useRef, useCallback } from 'react';
import { getComponentColor } from '../utils/colors';
import AnnotationTooltip from './AnnotationTooltip';

/**
 * Renders the SQL query with color-coded annotation spans.
 * Hovering a span shows a tooltip; clicking highlights the QEP tree node.
 */
export default function SQLAnnotatedView({
  query,
  annotations,
  hoveredIdx,
  setHoveredIdx,
  selectedIdx,
  setSelectedIdx,
  tooltipAnnotation,
  tooltipPos,
  setTooltipPos,
}) {
  const containerRef = useRef(null);

  // Build annotation spans sorted by position
  const spans = annotations
    .map((ann, idx) => ({
      start: ann.component.start_pos,
      end: ann.component.end_pos,
      idx,
      type: ann.component.component_type,
    }))
    .filter(s => !(s.start === 0 && s.end === 0)) // skip synthetic
    .sort((a, b) => a.start - b.start || b.end - a.end);

  // Remove overlapping spans: for each position, keep the first (outermost) span
  const activeSpans = [];
  let lastEnd = -1;
  for (const span of spans) {
    if (span.start >= lastEnd) {
      activeSpans.push(span);
      lastEnd = span.end;
    }
  }

  // Build React elements by walking through the query string
  const elements = [];
  let pos = 0;

  for (const span of activeSpans) {
    // Plain text before this span
    if (pos < span.start) {
      elements.push(
        <span key={`plain-${pos}`} className="sql-plain">
          {query.slice(pos, span.start)}
        </span>
      );
    }

    const color = getComponentColor(span.type);
    const isHovered = hoveredIdx === span.idx;
    const isSelected = selectedIdx === span.idx;

    elements.push(
      <span
        key={`ann-${span.idx}`}
        id={`sql-span-${span.idx}`}
        className={`sql-annotated ${isHovered ? 'hovered' : ''} ${isSelected ? 'selected' : ''}`}
        style={{
          backgroundColor: color.bg,
          borderColor: isHovered || isSelected ? color.border : 'transparent',
        }}
        onMouseEnter={(e) => {
          setHoveredIdx(span.idx);
          const rect = e.target.getBoundingClientRect();
          const containerRect = containerRef.current?.getBoundingClientRect() || { left: 0, top: 0 };
          setTooltipPos({
            x: rect.left - containerRect.left,
            y: rect.bottom - containerRect.top + 8,
          });
        }}
        onMouseLeave={() => {
          setHoveredIdx(null);
          setTooltipPos(null);
        }}
        onClick={() => setSelectedIdx(selectedIdx === span.idx ? null : span.idx)}
      >
        {query.slice(span.start, span.end)}
      </span>
    );

    pos = span.end;
  }

  // Remaining plain text
  if (pos < query.length) {
    elements.push(
      <span key={`plain-end`} className="sql-plain">
        {query.slice(pos)}
      </span>
    );
  }

  return (
    <div className="sql-annotated-view" ref={containerRef}>
      <h2>Annotated SQL Query</h2>
      <div className="sql-legend">
        {['scan', 'join', 'sort', 'aggregate', 'groupby', 'limit', 'having', 'filter'].map(type => {
          const c = getComponentColor(type);
          return (
            <span key={type} className="legend-item">
              <span className="legend-swatch" style={{ background: c.bg, borderColor: c.border }}/>
              {c.label}
            </span>
          );
        })}
      </div>
      <pre className="sql-code">{elements}</pre>
      <AnnotationTooltip annotation={tooltipAnnotation} position={tooltipPos} />
    </div>
  );
}
