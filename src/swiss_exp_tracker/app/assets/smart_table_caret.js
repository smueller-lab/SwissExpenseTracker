(function () {
    "use strict";

    // Dash's DataTable forces the edit-mode <input> to select its full value
    // on every mousedown while it already has focus, not just the click that
    // first enters edit mode — so a follow-up click meant to place the caret
    // partway through the text (to replace only part of a value) is silently
    // overridden back to a full-text selection. This isn't a single
    // synchronous reset: setting the caret once via setSelectionRange right
    // after the click is itself briefly correct, then gets stomped back to
    // full-select shortly after (React re-rendering the controlled <input>'s
    // value resets selection as a DOM side effect, on its own async timing we
    // don't control) — confirmed by polling selectionStart/End for ~2s after
    // a single corrective call, which stayed at the full range throughout.
    //
    // Fix: re-assert the intended caret position across several animation
    // frames after the click (not just once), stopping as soon as the user
    // types or moves focus/selection themselves. The click that *enters*
    // edit mode is left untouched — Dash's select-all-to-start-typing there
    // is intentional and not part of what's broken here.

    var SELECTOR = "#smart-table input.dash-cell-value";
    var CORRECTION_FRAMES = 15;
    var measureCanvas = null;

    function getCaretPosition(input, clickX) {
        if (!measureCanvas) {
            measureCanvas = document.createElement("canvas");
        }
        var context = measureCanvas.getContext("2d");
        var style = window.getComputedStyle(input);
        context.font =
            style.fontStyle +
            " " +
            style.fontWeight +
            " " +
            style.fontSize +
            " " +
            style.fontFamily;

        var text = input.value;
        var rect = input.getBoundingClientRect();
        var paddingLeft = parseFloat(style.paddingLeft) || 0;
        var paddingRight = parseFloat(style.paddingRight) || 0;
        var contentWidth = rect.width - paddingLeft - paddingRight;
        var totalTextWidth = context.measureText(text).width;

        // These inputs render right-aligned (Dash's own default) — short
        // values leave blank space before the text starts, so the text's
        // visual origin is offset from the input's left edge.
        var textStartX = 0;
        if (style.textAlign === "right") {
            textStartX = Math.max(0, contentWidth - totalTextWidth);
        }

        var relativeX = clickX - rect.left - paddingLeft - textStartX + input.scrollLeft;

        var bestPos = text.length;
        var bestDiff = Math.abs(totalTextWidth - relativeX);
        for (var i = 0; i < text.length; i++) {
            var widthBefore = context.measureText(text.slice(0, i)).width;
            var diff = Math.abs(widthBefore - relativeX);
            if (diff < bestDiff) {
                bestDiff = diff;
                bestPos = i;
            }
        }
        return bestPos;
    }

    function holdCaretPosition(input, pos) {
        var framesLeft = CORRECTION_FRAMES;
        var cancelled = false;

        function cancel() {
            cancelled = true;
            input.removeEventListener("keydown", cancel);
            input.removeEventListener("blur", cancel);
        }
        input.addEventListener("keydown", cancel);
        input.addEventListener("blur", cancel);

        function tick() {
            if (cancelled || document.activeElement !== input || framesLeft <= 0) {
                return;
            }
            framesLeft -= 1;
            if (input.selectionStart !== pos || input.selectionEnd !== pos) {
                input.setSelectionRange(pos, pos);
            }
            requestAnimationFrame(tick);
        }
        requestAnimationFrame(tick);
    }

    document.addEventListener(
        "mousedown",
        function (event) {
            var input = event.target.closest && event.target.closest(SELECTOR);
            if (!input) {
                return;
            }
            if (event.detail > 1) {
                return; // native double-click word-select stays untouched
            }
            if (document.activeElement !== input) {
                return; // this click is entering edit mode — leave Dash's select-all as-is
            }
            var clickX = event.clientX;
            var pos = getCaretPosition(input, clickX);
            holdCaretPosition(input, pos);
        },
        true
    );

    // Dash's DataTable has a long-standing, well-documented bug where
    // Backspace/Delete can corrupt its internal edit-tracking state so the
    // *committed* value ends up empty even though the input visibly still
    // shows (a reverted version of) the text — reproduced directly: type a
    // multi-character replacement, press Backspace once, and the value Dash
    // sends on commit is "" (previous committed value straight to empty),
    // never the correctly-trimmed string. Public reports of the same family
    // of bug: plotly/dash#2018, plotly/dash-table#700.
    //
    // Fix: take over Backspace/Delete entirely for these inputs — compute
    // the correct new value ourselves, apply it through React's native
    // input value setter (a plain `.value =` assignment is invisible to
    // React's change detection) and dispatch a real "input" event so Dash's
    // own (otherwise-correct, this bug is Backspace/Delete-specific) typing
    // sync picks it up — then block Dash's own broken handling from running
    // at all via preventDefault/stopPropagation.
    var nativeValueSetter = Object.getOwnPropertyDescriptor(
        window.HTMLInputElement.prototype,
        "value"
    ).set;

    function applyDeletion(input, newValue, newPos) {
        nativeValueSetter.call(input, newValue);
        input.dispatchEvent(new Event("input", { bubbles: true }));
        input.setSelectionRange(newPos, newPos);
    }

    document.addEventListener(
        "keydown",
        function (event) {
            if (event.key !== "Backspace" && event.key !== "Delete") {
                return;
            }
            var input = event.target.closest && event.target.closest(SELECTOR);
            if (!input) {
                return;
            }

            var start = input.selectionStart;
            var end = input.selectionEnd;
            var value = input.value;
            var newValue, newPos;

            if (start !== end) {
                newValue = value.slice(0, start) + value.slice(end);
                newPos = start;
            } else if (event.key === "Backspace") {
                if (start === 0) {
                    event.preventDefault();
                    event.stopPropagation();
                    return;
                }
                newValue = value.slice(0, start - 1) + value.slice(start);
                newPos = start - 1;
            } else {
                if (start >= value.length) {
                    event.preventDefault();
                    event.stopPropagation();
                    return;
                }
                newValue = value.slice(0, start) + value.slice(start + 1);
                newPos = start;
            }

            event.preventDefault();
            event.stopPropagation();
            applyDeletion(input, newValue, newPos);
        },
        true
    );
})();
