(function () {
    "use strict";

    var MOBILE_QUERY = "(max-width: 768px)";
    var mql = window.matchMedia(MOBILE_QUERY);

    var LAYOUT_DESKTOP = {
        legend: { orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "left", x: 0 },
        "margin.b": 40,
    };

    var LAYOUT_MOBILE = {
        legend: { orientation: "h", yanchor: "top", y: -0.35, xanchor: "center", x: 0.5 },
        "margin.b": 80,
    };

    function targetLayout() {
        return mql.matches ? LAYOUT_MOBILE : LAYOUT_DESKTOP;
    }

    function alreadyApplied(graphDiv, target) {
        var legend = (graphDiv.layout && graphDiv.layout.legend) || {};
        var margin = (graphDiv.layout && graphDiv.layout.margin) || {};
        return (
            legend.orientation === target.legend.orientation &&
            legend.yanchor === target.legend.yanchor &&
            legend.y === target.legend.y &&
            legend.xanchor === target.legend.xanchor &&
            legend.x === target.legend.x &&
            margin.b === target["margin.b"]
        );
    }

    // Only charts that actually render a legend get their bottom margin
    // pushed out on mobile — otherwise every chart would grow empty space.
    function applyLegendLayout(graphDiv) {
        if (!graphDiv || typeof Plotly === "undefined" || !graphDiv._fullLayout) {
            return;
        }
        if (!graphDiv._fullLayout.showlegend) {
            return;
        }
        var target = targetLayout();
        if (alreadyApplied(graphDiv, target)) {
            return;
        }
        Plotly.relayout(graphDiv, target);
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
