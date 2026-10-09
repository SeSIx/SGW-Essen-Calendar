"use strict";

// Comfort only: every page works without this file.
document.addEventListener("DOMContentLoaded", () => {
  const form = document.querySelector("form.event-form");
  if (form) {
    const allDay = form.querySelector("input[name=all_day]");
    const multiDay = form.querySelector("input[name=multi_day]");
    const sync = () => {
      form.querySelectorAll("[data-timed]").forEach((el) => { el.hidden = allDay.checked; });
      form.querySelectorAll("[data-multi]").forEach((el) => { el.hidden = !multiDay.checked; });
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
