import { useRef } from 'react';
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
    .sort((a, b) => a.start - b.start || a.end - b.end);

  // Remove overlapping spans: when two spans overlap, keep the shorter (more specific) one.
  // Non-overlapping spans are always kept.
  const activeSpans = [];
  for (const span of spans) {
    const last = activeSpans[activeSpans.length - 1];
    if (!last || span.start >= last.end) {
      // No overlap
      activeSpans.push(span);
    } else {
      // Overlap: keep the shorter (more specific) span
      const lastLen = last.end - last.start;
      const spanLen = span.end - span.start;
      if (spanLen < lastLen) {
        activeSpans[activeSpans.length - 1] = span;
      }
      // else keep the existing one
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

  // Only show legend items for annotation types actually present
  const presentTypes = new Set(annotations.map(a => a.component.component_type));
  const legendTypes = ['scan', 'join', 'sort', 'aggregate', 'groupby', 'limit', 'having', 'filter']
    .filter(t => presentTypes.has(t));

  return (
    <div className="sql-annotated-view" ref={containerRef}>
      <h2>Annotated SQL Query</h2>
      <div className="sql-legend">
        {legendTypes.map(type => {
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
