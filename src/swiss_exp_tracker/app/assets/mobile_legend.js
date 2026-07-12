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

    function wireGraph(graphDiv) {
        if (!graphDiv || graphDiv._mobileLegendWired) {
            return;
        }
        graphDiv._mobileLegendWired = true;
        graphDiv.on("plotly_afterplot", function () {
            applyLegendLayout(graphDiv);
        });
        applyLegendLayout(graphDiv);
    }

    function scanForGraphs(root) {
        if (!root || !root.querySelectorAll) {
            return;
        }
        root.querySelectorAll(".js-plotly-plot").forEach(wireGraph);
    }

    function applyToAllGraphs() {
        document.querySelectorAll(".js-plotly-plot").forEach(applyLegendLayout);
    }

    if (typeof mql.addEventListener === "function") {
        mql.addEventListener("change", applyToAllGraphs);
    } else if (typeof mql.addListener === "function") {
        mql.addListener(applyToAllGraphs);
    }

    // A resizing card can reflow how a legend wraps (more/fewer rows or
    // columns) even though its position doesn't change, which changes how
    // much margin it needs — recheck on any resize, not just the mobile
    // breakpoint the matchMedia listener already covers.
    var resizeTimer;
    window.addEventListener("resize", function () {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(applyToAllGraphs, 150);
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
