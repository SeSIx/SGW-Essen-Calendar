"use strict";

// Comfort only: every page works without this file.
document.addEventListener("DOMContentLoaded", () => {
  const form = document.querySelector("form.event-form");
  if (form) {
    const allDay = form.querySelector("input[name=all_day]");
    const multiDay = form.querySelector("input[name=multi_day]");
    const sync = () => {
      // Hidden inputs are disabled too, so they are not sent and cannot clash with the switches.
      const show = (selector, visible) => form.querySelectorAll(selector).forEach((el) => {
        el.hidden = !visible;
        el.querySelectorAll("input").forEach((input) => { input.disabled = !visible; });
      });
      show("[data-timed]", !allDay.checked);
      show("[data-multi]", multiDay.checked);
    };
    allDay.addEventListener("change", sync);
    multiDay.addEventListener("change", sync);
    sync();
  }

  // A second tap must not send a form twice. Disable after the submit has started,
  // so the browser still sends the form.
  document.querySelectorAll("form").forEach((f) => {
    f.addEventListener("submit", () => {
      setTimeout(() => f.querySelectorAll("button[type=submit]").forEach((b) => { b.disabled = true; }), 0);
    });
  });

  document.querySelectorAll(".notice").forEach((el) => {
    setTimeout(() => { el.hidden = true; }, 6000);
  });
});
