(function () {
    "use strict";

    // Wire a single .trip-pool-row: mark draggable, attach dragstart, and
    // attach a click-to-assign handler (clicking a row assigns it to
    // whichever trip's dropzone is currently visible — the Bucket Builder
    // only ever shows one trip's dropzone at a time). Both paths funnel
    // through the same "trips-drop-store" write so the server-side handling
    // is identical for a drag-drop and a click.
    // The _tripDragWired flag prevents double-binding after Dash re-renders
    // replace DOM nodes and wipe previously attached listeners.
    function wireRow(el) {
        if (!el || el._tripDragWired) {
            return;
        }
        el._tripDragWired = true;
        el.draggable = true;
        el.addEventListener("dragstart", function (event) {
            event.dataTransfer.setData("text/plain", el.dataset.txId);
        });
        el.addEventListener("click", function () {
            var dropzone = document.querySelector(".trip-bucket-dropzone");
            if (!dropzone || !dropzone.dataset.tripId) {
                return;
            }
            if (window.dash_clientside && window.dash_clientside.set_props) {
                window.dash_clientside.set_props("trips-drop-store", {
                    data: {
                        tx_id: el.dataset.txId,
                        trip_id: dropzone.dataset.tripId,
                        ts: Date.now(),
                    },
                });
            }
        });
    }

    // Wire a single .trip-bucket-dropzone: attach dragover / dragleave / drop.
    // drop writes to the Dash store so callbacks can react without a full
    // server round-trip; ts is included so the Input fires even on a repeat
    // drop of the same (tx_id, trip_id) pair (otherwise the store payload
    // would be identical and Dash would not trigger the callback again).
    function wireDropzone(el) {
        if (!el || el._tripDropzoneWired) {
            return;
        }
        el._tripDropzoneWired = true;
        el.addEventListener("dragover", function (event) {
            event.preventDefault();
            el.classList.add("drag-over");
        });
        el.addEventListener("dragleave", function () {
            el.classList.remove("drag-over");
        });
        el.addEventListener("drop", function (event) {
            event.preventDefault();
            el.classList.remove("drag-over");
            var tx_id = event.dataTransfer.getData("text/plain");
            var trip_id = el.dataset.tripId;
            if (window.dash_clientside && window.dash_clientside.set_props) {
                window.dash_clientside.set_props("trips-drop-store", {
                    data: { tx_id: tx_id, trip_id: trip_id, ts: Date.now() },
                });
            }
        });
    }

    // Scan a subtree for any unwired .trip-pool-row elements and wire them.
    function scanForRows(root) {
        if (!root || !root.querySelectorAll) {
            return;
        }
        root.querySelectorAll(".trip-pool-row").forEach(wireRow);
    }

    // Scan a subtree for any unwired .trip-bucket-dropzone elements and wire them.
    function scanForDropzones(root) {
        if (!root || !root.querySelectorAll) {
            return;
        }
        root.querySelectorAll(".trip-bucket-dropzone").forEach(wireDropzone);
    }

    // Dash re-renders may add new trip rows or dropzones at any depth —
    // observe both new child nodes and class attribute changes (Dash
    // sometimes writes classes onto existing nodes rather than replacing them).
    var observer = new MutationObserver(function (mutations) {
        mutations.forEach(function (mutation) {
            if (mutation.type === "attributes") {
                var target = mutation.target;
                if (target.nodeType !== 1 || !target.classList) {
                    return;
                }
                if (target.classList.contains("trip-pool-row")) {
                    wireRow(target);
                } else if (target.classList.contains("trip-bucket-dropzone")) {
                    wireDropzone(target);
                }
                return;
            }
            mutation.addedNodes.forEach(function (node) {
                if (node.nodeType !== 1) {
                    return;
                }
                if (node.classList && node.classList.contains("trip-pool-row")) {
                    wireRow(node);
                } else if (
                    node.classList &&
                    node.classList.contains("trip-bucket-dropzone")
                ) {
                    wireDropzone(node);
                }
                scanForRows(node);
                scanForDropzones(node);
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
        scanForRows(document);
        scanForDropzones(document);
        setInterval(function () {
            scanForRows(document);
            scanForDropzones(document);
        }, 1000);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", startObserving);
    } else {
        startObserving();
    }
})();
