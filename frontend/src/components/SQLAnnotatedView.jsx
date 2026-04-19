import { useRef } from 'react';
import { getComponentColor } from '../utils/colors';
import AnnotationTooltip from './AnnotationTooltip';

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
  setPinnedPos,
}) {
  const containerRef = useRef(null);

  const spans = annotations
    .map((ann, idx) => ({
      start: ann.component.start_pos,
      end: ann.component.end_pos,
      idx,
      type: ann.component.component_type,
    }))
    .filter(s => !(s.start === 0 && s.end === 0)) // skip synthetic
    .sort((a, b) => a.start - b.start || a.end - b.end);

  // On overlap, keep the shorter (more specific) span
  const activeSpans = [];
  for (const span of spans) {
    const last = activeSpans[activeSpans.length - 1];
    if (!last || span.start >= last.end) {
      activeSpans.push(span);
    } else {
      const lastLen = last.end - last.start;
      const spanLen = span.end - span.start;
      if (spanLen < lastLen) {
        activeSpans[activeSpans.length - 1] = span;
      }
    }
  }

  const elements = [];
  let pos = 0;

  for (const span of activeSpans) {
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
        onClick={(e) => {
          const unpin = selectedIdx === span.idx;
          setSelectedIdx(unpin ? null : span.idx);
          if (unpin) {
            if (setPinnedPos) setPinnedPos(null);
          } else if (setPinnedPos) {
            // Anchor the pinned tooltip at the clicked span's current position
            // (mouseenter already set tooltipPos, but we capture it separately so
            // it survives the onMouseLeave clear).
            const rect = e.currentTarget.getBoundingClientRect();
            const containerRect = containerRef.current?.getBoundingClientRect() || { left: 0, top: 0 };
            setPinnedPos({
              x: rect.left - containerRect.left,
              y: rect.bottom - containerRect.top + 8,
            });
          }
        }}
      >
        {query.slice(span.start, span.end)}
      </span>
    );

    pos = span.end;
  }

  if (pos < query.length) {
    elements.push(
      <span key={`plain-end`} className="sql-plain">
        {query.slice(pos)}
      </span>
    );
  }

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
      <p className="sql-usage-note">
        Note: Hover a highlighted region to see its annotation. Click to pin the highlight.
      </p>
      <pre className="sql-code">{elements}</pre>
      <AnnotationTooltip annotation={tooltipAnnotation} position={tooltipPos} />
    </div>
  );
}
