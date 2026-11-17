(function () {
    "use strict";

    // Drag handles at the bottom of the Bucket Builder cards. Dragging sets a
    // min-height on the shared .trips-builder-grid, so both cards in the row
    // stretch together (and the unassigned-transactions table grows with
    // them). The height is kept in localStorage so it survives reloads;
    // double-clicking a handle clears it. Below the 1024px breakpoint the
    // cards stack (see style.css), so the saved height is not applied there.
    var STORAGE_KEY = "trips-builder-height";
    var WIDE_QUERY = window.matchMedia("(min-width: 1025px)");

    function readHeight() {
        try {
            var value = parseInt(window.localStorage.getItem(STORAGE_KEY), 10);
            return isNaN(value) ? null : value;
        } catch (err) {
            return null;
        }
    }

    function writeHeight(value) {
        try {
            if (value === null) {
                window.localStorage.removeItem(STORAGE_KEY);
            } else {
                window.localStorage.setItem(STORAGE_KEY, String(value));
            }
        } catch (err) {
            // Storage blocked (private window etc.) — resize still works per page view.
        }
    }

    // Apply the saved height to a grid, or clear it on narrow screens.
    function applyHeight(grid) {
        var saved = readHeight();
        grid.style.minHeight = saved !== null && WIDE_QUERY.matches ? saved + "px" : "";
    }

    // Wire a single .trip-resize-handle: pointer drag resizes its grid,
    // double-click resets. _tripResizeWired prevents double-binding after
    // Dash re-renders (same pattern as trip_dragdrop.js).
    function wireHandle(handle) {
        if (!handle || handle._tripResizeWired) {
            return;
        }
        var grid = handle.closest(".trips-builder-grid");
        if (!grid) {
            return;
        }
        handle._tripResizeWired = true;
        applyHeight(grid);

        handle.addEventListener("pointerdown", function (event) {
            if (!WIDE_QUERY.matches) {
                return;
            }
            event.preventDefault();
            handle.setPointerCapture(event.pointerId);
            var startY = event.clientY;
            var startHeight = grid.getBoundingClientRect().height;
            grid.classList.add("is-resizing");

            function onMove(moveEvent) {
                var height = Math.round(startHeight + moveEvent.clientY - startY);
                grid.style.minHeight = height + "px";
            }

            function onUp() {
                handle.removeEventListener("pointermove", onMove);
                handle.removeEventListener("pointerup", onUp);
                handle.removeEventListener("pointercancel", onUp);
                grid.classList.remove("is-resizing");
                writeHeight(Math.round(grid.getBoundingClientRect().height));
            }

            handle.addEventListener("pointermove", onMove);
            handle.addEventListener("pointerup", onUp);
            handle.addEventListener("pointercancel", onUp);
        });

        handle.addEventListener("dblclick", function () {
            writeHeight(null);
            grid.style.minHeight = "";
        });
    }

    function scanForHandles(root) {
        if (!root || !root.querySelectorAll) {
            return;
        }
        root.querySelectorAll(".trip-resize-handle").forEach(wireHandle);
    }

    // Dash renders the page (and re-renders tabs) after load, so watch for
    // handles being added anywhere in the document.
    var observer = new MutationObserver(function (mutations) {
        mutations.forEach(function (mutation) {
            mutation.addedNodes.forEach(function (node) {
                if (node.nodeType !== 1) {
                    return;
                }
                if (node.classList && node.classList.contains("trip-resize-handle")) {
                    wireHandle(node);
                }
                scanForHandles(node);
            });
        });
    });

    function startObserving() {
        observer.observe(document.body, { childList: true, subtree: true });
        scanForHandles(document);
        WIDE_QUERY.addEventListener("change", function () {
            document.querySelectorAll(".trips-builder-grid").forEach(applyHeight);
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", startObserving);
    } else {
        startObserving();
    }
})();
