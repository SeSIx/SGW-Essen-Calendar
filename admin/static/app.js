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

  document.querySelectorAll("button[data-place]").forEach((button) => {
    button.addEventListener("click", () => {
      const input = button.closest("form").querySelector("input[name=location]");
      input.value = button.dataset.place;
      input.focus();
    });
  });

  const chips = document.querySelector("nav.chips[data-filter-url]");
  if (chips) initTeamFilter(chips);

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

// Chips stay real links (no JS: they reload with ?teams=). With JS a tap only shows or hides
// the already rendered games, and the choice is saved in the background.
function initTeamFilter(nav) {
  const chipEls = [...nav.querySelectorAll("a.chip[data-team]")];
  const slugs = chipEls.map((chip) => chip.dataset.team);
  const past = new URLSearchParams(location.search).get("frueher") === "1";
  const hrefFor = (selected) => {
    const params = new URLSearchParams({ teams: slugs.filter((s) => selected.has(s)).join(",") });
    if (past) params.set("frueher", "1");
    return "?" + params.toString();
  };
  const selection = () => new Set(chipEls.filter((c) => c.classList.contains("on")).map((c) => c.dataset.team));

  const render = (selected) => {
    chipEls.forEach((chip) => {
      const on = selected.has(chip.dataset.team);
      chip.classList.toggle("on", on);
      chip.setAttribute("aria-pressed", String(on));
      const toggled = new Set(selected);
      if (on) toggled.delete(chip.dataset.team); else toggled.add(chip.dataset.team);
      chip.setAttribute("href", hrefFor(toggled));
    });
    document.querySelectorAll("li[data-team]").forEach((li) => { li.hidden = !selected.has(li.dataset.team); });
    const today = document.querySelector(".today");
    let first = true;
    document.querySelectorAll("section[data-month]").forEach((section) => {
      const visible = [...section.querySelectorAll("li")].some((li) => !li.hidden);
      section.hidden = !visible;
      if (visible && first) {
        first = false;
        if (today) section.querySelector("h2.month").append(today);
      }
    });
    const none = document.querySelector("[data-no-teams]");
    if (none) none.hidden = selected.size > 0;
    const empty = document.querySelector("[data-empty]");
    if (empty) empty.hidden = !first;
    history.replaceState(null, "", hrefFor(selected));
  };

  const save = (selected) => {
    const body = new URLSearchParams({
      teams: slugs.filter((s) => selected.has(s)).join(","),
      csrf_token: nav.dataset.csrf,
    });
    // Best effort: the view is already updated, and the URL carries the choice on reload.
    fetch(nav.dataset.filterUrl, { method: "POST", body, credentials: "same-origin" }).catch(() => {});
  };

  chipEls.forEach((chip) => {
    chip.addEventListener("click", (event) => {
      if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey) return;
      event.preventDefault();
      const selected = selection();
      const slug = chip.dataset.team;
      if (selected.has(slug)) selected.delete(slug); else selected.add(slug);
      render(selected);
      save(selected);
    });
  });
}
