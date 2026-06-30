# ONOV8 Data Console — UI/UX Redesign Notes

**Date:** 2026-05-18
**Scope:** Frontend visual system only — no backend, API, route, runtime, or Docker changes.

---

## What changed

A full CSS rewrite of `dashboard/web/src/styles.css`, anchored on a token-driven design system. The previous file (`styles.legacy.css.bak`) is preserved in the same directory in case of rollback.

- Lines before: **4,895**
- Lines after: **5,393** (more thorough, better organized)
- Tokens defined: **112 CSS variables**
- Token references: **~1,375 `var(--…)` calls**
- Hardcoded hex values outside `:root`: **0**

The JSX (`main.jsx`) was **not modified**. Every existing class name continues to render — the visual upgrade is purely CSS-driven.

---

## Design language

**Direction:** Premium enterprise dark theme (Linear / Databricks / Grafana family).

**Color foundation:**
- App surface: `#0a0c11` (near-black with subtle blue cast)
- Surface → Elevated → Overlay layering (`#11141c` → `#161a23` → `#1b1f2a`)
- Borders: subtle (`#1c2030`) → default (`#252b3a`) → strong (`#353b4f`)
- Text: primary (`#e7e9f0`) → secondary (`#a4abbd`) → muted (`#707689`)
- **Accent:** Linear-style indigo `#5b6cf5`

**Semantic status:**
- Good — emerald `#10b981`
- Warn — amber `#f59e0b`
- Bad — red `#ef4444`
- Info — blue `#3b82f6`
- All have matching `-soft`, `-border`, `-text` variants for layered application.

**Type:** Inter sans + JetBrains Mono. 11px → 32px scale, with `font-variant-numeric: tabular-nums` applied to all metrics.

**Radius scale:** 4 / 6 / 8 / 10 / 14 / pill.
**Spacing scale:** 2 → 32 px (12 stops).
**Shadows:** subtle, dark-mode-tuned, never flashy.

---

## Visual systems redesigned

| System | Status | Notes |
|---|---|---|
| Tokens (`:root`) | New | 112 design tokens — colors, radii, spacing, type, shadows, motion, sidebar, status |
| App shell | Redesigned | Unified dark background; sticky sidebar; refined topbar with bottom border |
| Sidebar | Redesigned | Compact 248px; group section labels (uppercase, 10px, tracked); active item gets `accent-soft` background, accent-colored icon, and a 3px left rail; brand mark uses an indigo gradient with inner highlight |
| Topbar | Redesigned | Title 22px / 600; subline with badges; session pill; refined icon buttons; bottom border separator |
| Panels (`.panel`) | Redesigned | Surface bg, subtle border, restrained radius (10px), `:hover` border lift |
| Metrics (`.metric`) | Redesigned | Uppercase tracked label, tabular-num value, consistent 96px min-height |
| Badges | Redesigned | Pill, semantic soft+border+text triplets, capitalize, tabular legibility |
| Buttons | Redesigned | Default (elevated bg), Primary (accent), Danger (red), Icon — all share borders/radius, animated bg+border on hover, accessible focus ring |
| Inputs / selects / textareas | Redesigned | Dark inputs, custom SVG select chevron, accent focus ring |
| Tabs | Redesigned | Container-style "segmented control" (Linear-style) with active pill |
| Tables | Redesigned | Sticky uppercase headers, hover rows, subtle row borders, tabular numerals |
| Modals / Dialogs / Toasts | Redesigned | Backdrop blur, elevated dark surface, animated entrance, semantic-color toast accents |
| Runtime status chips | Redesigned | Glow rings on `running` / `partial` / `error` dots |
| Runtime Control Center | Redesigned | Card facts, action output blocks, warning rows, URL chips |
| Progress bars | Redesigned | 8px track, animated sheen, semantic gradient fills |
| Source / Raw / Bronze / Silver cards | Redesigned | Hierarchical card families with consistent paddings, accent borders for status |
| Danger zones | Redesigned | Distinct red-tinted surfaces with 3px left rail, dedicated kicker chip, calm but assertive |
| Empty states | Redesigned | Dashed border, centered, calm muted text |
| Lineage / flow / nodes | Redesigned | Dark elevated surfaces, semantic `.good` variants |
| Setup wizard | Redesigned | Sticky stepper, accent-soft active step, conic-gradient score ring |
| User Guide | Redesigned | Hero panels with radial accent glow, sticky TOC, accent-on-hover page cards |
| End-to-End flow | Redesigned | Workbench split, selectable collection rows with accent-soft selection, semantic step cards |
| Responsive | Preserved + improved | 1200 / 1024 / 760px breakpoints |

---

## Files touched

- `dashboard/web/src/styles.css` — full rewrite + light-theme block (~5,560 lines)
- `dashboard/web/src/styles.legacy.css.bak` — backup of the original CSS for rollback
- `dashboard/web/src/main.jsx` — **three surgical edits only:**
  - Added `Sun, Moon` to the existing `lucide-react` import
  - Added `theme` state + `useEffect` that sets `data-theme="light"` on `<html>` and persists `onov8-theme` in `localStorage` (defaults to `dark`)
  - Inserted one `IconButton` toggle into the existing `.top-actions` row, before Refresh

No API, route, runtime, Docker, or backend changes.

## Light / Dark theme

A complete `[data-theme="light"]` token override block now lives in `styles.css` (§29). Every component CSS rule consumes tokens — no component file knows about themes. Toggling is done via the topbar Sun/Moon button (preference persisted in `localStorage`).

Light-theme highlights:
- App / surface go to white / cool-gray (`#ffffff` → `#f7f8fb`)
- Sidebar inverts to a light surface with darker text (Linear/Notion style, not the dated "dark sidebar in light mode" look)
- Accent shifts one stop darker (`#4554e8`) for legibility on white
- Status colors all deepened (good `#047857`, warn `#92400e`, bad `#b91c1c`, info `#1d4ed8`) for contrast on light surfaces
- Shadows become soft layered black at low opacity
- Smooth `transition` applied to the heavily-themed surfaces (panels, cards, inputs, buttons, badges) so the swap doesn't flash

---

## Things to verify locally

1. Run `npm run dev` (or `vite build` then preview).
2. Walk through Overview → Sources → Raw → Bronze → Silver → Query → BI → Governance → Operations → Runtime Control Center → Alerts → Setup Wizard → User Guide.
3. Confirm the sidebar active state has the accent rail on the left and accent-soft background.
4. Confirm the runtime chips in the topbar pulse with status-colored glows.
5. Confirm the Bronze / Raw danger zones still read clearly as destructive (red rail + soft red bg).
6. Confirm tables have hover rows and sticky uppercase headers.
7. Confirm the toast slides in from below-right with the correct semantic accent.

---

## Remaining UI debt / future polish recommendations

These were intentionally left alone to keep the scope clean. They are good follow-up work:

1. **Sidebar collapse.** Add a collapse-to-rail mode (icons only, 56px wide) for power users. Would require a small JSX change + state.
2. **Light theme.** All tokens already exist in `:root`. A light theme is now a single class swap (`.theme-light`) away with overridden variables. Worth doing once a designer reviews accent contrast.
3. **Iconography polish.** Lucide is solid, but some pages mix icon weights (16px / 17px / 18px). A pass through `IconButton`, `TextButton`, and inline icons to standardize on 14-16px would tighten things further.
4. **Empty-state illustrations.** Replace the dashed placeholders on layer-offline / no-data states with small Lucide icon compositions.
5. **Keyboard shortcuts overlay.** This console begs for `⌘K` command palette. Out of scope here, but the visual system is ready for it.
6. **Density toggle.** Some pages would benefit from a `compact` mode that reduces padding by ~25%. Hooks already exist on selectors like `.silver-page` and `.runtime-control-page`.
7. **Status semantics audit.** `statusClass()` maps ~80 statuses to four buckets. Worth a periodic audit to ensure every new status string gets categorized.
8. **Print stylesheet.** Currently very minimal. If anyone exports / prints alerts or audit reports, expand the print rules.
9. **Reduced-motion fallback.** `prefers-reduced-motion` is honored on source-status icons. Worth extending to the toast slide-in and progress sheen.

---

## How to roll back

```bash
cd dashboard/web/src
cp styles.legacy.css.bak styles.css
```

The legacy stylesheet is byte-for-byte preserved.
