(function () {
    "use strict";

    // Matches style.css's own breakpoint where cards stop sitting side-by-side
    // and stack to full width (see ".col-*" rules under this media query).
    // Below it, a card is effectively single-column already, so a right-side
    // legend has no spare width to sit in without crushing the plot itself
    // (skipped axis labels, thin bars) — keep this in sync with style.css.
    var MOBILE_QUERY = "(max-width: 1024px)";
    var mql = window.matchMedia(MOBILE_QUERY);

    // Matches myTemp's default margins (see ploty_template.py) — the floor
    // below which a margin never shrinks, even when no legend is rendered.
    var BASE_MARGIN = { b: 40, r: 40 };

    // Breathing room (px) between the rendered legend and the plot area.
    var LEGEND_GAP = 12;

    // Desktop/tablet always gets the vertical right-side legend — it's also
    // each figure's Python-set default (see plots.md), so on non-mobile this
    // matches on first paint and needs no repositioning (no flash). Only
    // mobile gets a horizontal legend below the plot; a "top" position is
    // deliberately not used at all, keeping this to two straightforward cases
    // instead of a width/item-count-based fallback that flip-flopped and was
    // fragile — Plotly's own scrollbar handles legends too tall to fit.
    // side: which margin the legend eats into ("b" below / "r" right).
    function legendPosition() {
        if (mql.matches) {
            return {
                orientation: "h",
                yanchor: "top",
                y: -0.35,
                xanchor: "center",
                x: 0.5,
                side: "b",
            };
        }
        return {
            orientation: "v",
            yanchor: "middle",
            y: 0.5,
            xanchor: "left",
            x: 1.02,
            side: "r",
        };
    }

    function positionApplied(graphDiv, target) {
        var legend = (graphDiv.layout && graphDiv.layout.legend) || {};
        return (
            legend.orientation === target.orientation &&
            legend.yanchor === target.yanchor &&
            legend.y === target.y &&
            legend.xanchor === target.xanchor &&
            legend.x === target.x
        );
    }

    // Plotly has no `legend.automargin` (unlike axes/titles) — a legend that
    // wraps to multiple rows/columns (many categories) does not push the
    // plot area out of the way on its own, and overlaps it instead. Measure
    // the rendered legend's actual size and reserve that much margin.
    function measuredLegendSize(graphDiv, side) {
        var node = graphDiv.querySelector("g.legend");
        if (!node) {
            return 0;
        }
        var rect = node.getBoundingClientRect();
        return side === "r" ? rect.width : rect.height;
    }

    // Plotly.relayout()/react() are not safe to overlap on the same graphDiv:
    // firing one while a previous call is still resolving (e.g. our relayout
    // racing the data-filling callback's own Plotly.react()) can leave the
    // SVG paint stuck mid-update — correct layout numbers, collapsed visual.
    // Track in-flight calls per graphDiv and skip re-entrant relayouts; the
    // in-flight call's own afterplot will re-run this once it settles.
    function relayout(graphDiv, update) {
        if (graphDiv._legendRelayoutBusy) {
            return;
        }
        graphDiv._legendRelayoutBusy = true;
        Plotly.relayout(graphDiv, update).finally(function () {
            graphDiv._legendRelayoutBusy = false;
        });
    }

    function applyLegendLayout(graphDiv) {
        if (!graphDiv || typeof Plotly === "undefined" || !graphDiv._fullLayout) {
            return;
        }
        if (graphDiv._legendRelayoutBusy) {
            return;
        }
        if (!graphDiv._fullLayout.showlegend) {
            return;
        }
        // Opt-out for the rare figure that sets its own fixed legend
        // position in Python (layout.meta.legendFixed) instead of the
        // site-wide responsive right/below rule — leave it exactly as
        // Python set it, at every breakpoint.
        if (graphDiv._fullLayout.meta && graphDiv._fullLayout.meta.legendFixed) {
            return;
        }

        var target = legendPosition();
        if (!positionApplied(graphDiv, target)) {
            relayout(graphDiv, {
                legend: {
                    orientation: target.orientation,
                    yanchor: target.yanchor,
                    y: target.y,
                    xanchor: target.xanchor,
                    x: target.x,
                },
            });
            return; // the relayout above triggers another afterplot to measure against
        }

        var measured = measuredLegendSize(graphDiv, target.side);
        if (!measured) {
            return;
        }
        var base = BASE_MARGIN[target.side];
        var needed = Math.max(base, Math.round(measured + LEGEND_GAP));
        var current = (graphDiv.layout.margin && graphDiv.layout.margin[target.side]) || base;
        if (Math.abs(current - needed) > 1) {
            var update = {};
            update["margin." + target.side] = needed;
            relayout(graphDiv, update);
        }
    }

    // GRAPH_CONFIG sets responsive:true, so Plotly runs its own internal
    // resize pass (via a ResizeObserver on the container) independently of
    // this file. That native pass doesn't touch `_legendRelayoutBusy` at
    // all, so calling our relayout directly from an outside trigger (initial
    // discovery, a matchMedia change) can land while Plotly's own pass is
    // still in flight — two concurrent updates to the same graphDiv, which
    // is the same "stuck mid-update, collapsed visual" failure mode as the
    // relayout/react race above. Deferring two animation frames lets the
    // browser's current layout/paint pass (and any native resize it
    // triggered) settle first before we touch the graph ourselves.
    function nextFrame(fn) {
        requestAnimationFrame(function () {
            requestAnimationFrame(fn);
        });
    }

    // Every figure sets a fixed, non-data-dependent height in Python (see
    // plots.md) — it is never supposed to change after first paint, on a
    // Month/Year toggle, a dropdown filter, or anything else. But
    // GRAPH_CONFIG's responsive:true measures each graph's *container* on
    // every resize and overwrites layout.height to match it, silently
    // breaking that invariant — and if the container is itself stretched to
    // match a taller sibling in the same CSS grid row (cards intentionally
    // match row height, see style.css .card), the grown figure grows the
    // row further, which stretches the card further, forever. Reproduced
    // with headless Chrome (a fast multi-step resize sequence, e.g.
    // dragging the window edge).
    //
    // Fix: trust the height Plotly renders on the very first afterplot for
    // a graphDiv (it is exactly what Python passed in — responsive:true
    // cannot have drifted it yet) and snap back to that recorded height on
    // every subsequent afterplot where it no longer matches. This does not
    // touch width, so Plotly's own responsive column-width fitting is
    // unaffected — only height is ever pinned.
    function enforceFixedHeight(graphDiv) {
        if (!graphDiv || typeof Plotly === "undefined" || !graphDiv._fullLayout) {
            return;
        }
        if (graphDiv._legendRelayoutBusy) {
            return;
        }
        if (typeof graphDiv._intendedHeight !== "number") {
            graphDiv._intendedHeight = graphDiv._fullLayout.height;
            return;
        }
        if (graphDiv._fullLayout.height !== graphDiv._intendedHeight) {
            relayout(graphDiv, { height: graphDiv._intendedHeight });
        }
    }

    function wireGraph(graphDiv) {
        if (!graphDiv || graphDiv._mobileLegendWired) {
            return;
        }
        graphDiv._mobileLegendWired = true;
        graphDiv.on("plotly_afterplot", function () {
            enforceFixedHeight(graphDiv);
            applyLegendLayout(graphDiv);
        });
        nextFrame(function () {
            enforceFixedHeight(graphDiv);
            applyLegendLayout(graphDiv);
        });
    }

    function scanForGraphs(root) {
        if (!root || !root.querySelectorAll) {
            return;
        }
        root.querySelectorAll(".js-plotly-plot").forEach(wireGraph);
    }

    // SPIKE (fix/legend-pos): clientside_callback-driven replacement for the
    // MutationObserver below, scoped for now to graphs using the
    // {"type": "chart-graph", ...} pattern-matching id (see callbacks/legend.py).
    // Dash re-fires a pattern-matching ALL callback when the matching
    // component set changes (mount/unmount), not just on prop updates, so
    // this Input({"type":"chart-graph","index":ALL}, "figure") callback fires
    // once a migrated graph first mounts — same discovery job the observer
    // does, but driven by Dash's own dependency graph instead of DOM
    // watching. Once every graph is migrated, the observer/poll below can be
    // deleted and this becomes the only discovery mechanism.
    window.dash_clientside = window.dash_clientside || {};
    window.dash_clientside.legend = window.dash_clientside.legend || {};
    window.dash_clientside.legend.rescan = function () {
        scanForGraphs(document);
        return window.dash_clientside.no_update;
    };

    function applyToAllGraphs() {
        document.querySelectorAll(".js-plotly-plot").forEach(applyLegendLayout);
    }

    // Breakpoint crossing is a case Plotly's own resize can't be relied on
    // for: a card that's already full-width on both sides of 1024px doesn't
    // change container size when the breakpoint is crossed, so Plotly's
    // ResizeObserver may not fire at all — matchMedia is the only signal.
    if (typeof mql.addEventListener === "function") {
        mql.addEventListener("change", function () {
            nextFrame(applyToAllGraphs);
        });
    } else if (typeof mql.addListener === "function") {
        mql.addListener(function () {
            nextFrame(applyToAllGraphs);
        });
    }

    // dcc.Graph's own resize handling (separate from — and in addition to —
    // Plotly's responsive:true) can get stuck when resize events fire in
    // rapid succession, e.g. a user actually dragging the window edge
    // rather than a single discrete resize: verified by reproducing it
    // (headless Chrome, a fast multi-step resize sequence) — a graph gets
    // locked in at an intermediate size from mid-drag that never corrects
    // itself even once the window stops moving and settles at its final
    // size, permanently mismatched with its actual container until
    // something forces a fresh resize. A single corrective
    // Plotly.Plots.resize() once resize events stop firing (debounced, not
    // per-event, so it runs well after Plotly's own native handling has had
    // time to settle rather than racing it) fixes the mismatch.
    var resizeSettleTimer;
    window.addEventListener("resize", function () {
        clearTimeout(resizeSettleTimer);
        resizeSettleTimer = setTimeout(function () {
            document.querySelectorAll(".js-plotly-plot").forEach(function (graphDiv) {
                if (typeof Plotly === "undefined" || !graphDiv._fullLayout) {
                    return;
                }
                Plotly.Plots.resize(graphDiv);
            });
        }, 250);
    });

    // react-plotly.js calls Plotly.newPlot() on a <div> React already mounted,
    // which adds the "js-plotly-plot" class via a class-attribute write, not a
    // new node — so childList alone never sees a graph appear. Watch the class
    // attribute too, and poll as a cheap belt-and-braces fallback.
    var observer = new MutationObserver(function (mutations) {
        mutations.forEach(function (mutation) {
            if (mutation.type === "attributes") {
                var target = mutation.target;
                if (
                    target.nodeType === 1 &&
                    target.classList &&
                    target.classList.contains("js-plotly-plot")
                ) {
                    wireGraph(target);
                }
                return;
            }
            mutation.addedNodes.forEach(function (node) {
                if (node.nodeType !== 1) {
                    return;
                }
                if (node.classList && node.classList.contains("js-plotly-plot")) {
                    wireGraph(node);
                }
                scanForGraphs(node);
            });
        });
    });

    function startObserving() {
        observer.observe(document.body, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ["class"],
        });
        scanForGraphs(document);
        setInterval(function () {
            scanForGraphs(document);
        }, 1000);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", startObserving);
    } else {
        startObserving();
    }
})();
