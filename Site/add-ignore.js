"use strict";

(function () {
  const FACTORY_SOURCES = {
    JV_F_P: {
      label: "JV_F_P — каталоги",
      path: "../Fabriks/JV_F_P/factories.json",
    },
    XL_F_P: {
      label: "XL_F_P — каталоги",
      path: "../Fabriks/XL_F_P/factories.json",
    },
    JV_F_L: {
      label: "JV_F_L — листер",
      path: "../Fabriks/JV_F_L/collections.json",
    },
    XL_F_L: {
      label: "XL_F_L — листер",
      path: "../Fabriks/XL_F_L/collections.json",
    },
  };

  const IGNORE_SOURCES = {
    JV_F_P: "../Ignore/JV_P.json",
    XL_F_P: "../Ignore/XL_P.json",
    JV_F_L: "../Ignore/JV_L.json",
    XL_F_L: "../Ignore/XL_L.json",
  };

  const TYPE_LABELS = Object.fromEntries(
    Object.entries(FACTORY_SOURCES).map(([type, { label }]) => [type, label])
  );

  const overviewGrid = document.querySelector("[data-overview-grid]");
  const form = document.querySelector(
    '.command-form[data-role="add-ignore-form"]'
  );
  const selectedList = form?.querySelector("[data-selected-list]");
  const emptyState = form?.querySelector("[data-empty-state]");
  const openPickerButton = form?.querySelector("[data-open-picker]");
  const overwriteCheckbox =
    form?.querySelector('input[name="overwrite-name"]');
  const applyButton = form?.querySelector("[data-apply-ignore]");

  const resultPanel = document.querySelector("[data-result-panel]");
  const resultList = document.querySelector("[data-result-list]");
  const clearResultButton = document.querySelector("[data-clear-result]");

  const picker = document.getElementById("factory-picker");
  const pickerList = picker?.querySelector("[data-picker-list]");
  const pickerSearch = picker?.querySelector("[data-picker-search]");
  const pickerType = picker?.querySelector("[data-picker-type]");
  const pickerIgnoreFilter = picker?.querySelector("[data-picker-ignore]");
  const pickerCloseButtons = picker?.querySelectorAll("[data-picker-close]");

  const state = {
    factories: [],
    selections: [],
    ignoreSet: new Set(),
    loadErrors: [],
    isPickerOpen: false,
    isApplying: false,
  };

  function keyFor(type, id) {
    return `${type}::${id}`;
  }

  async function fetchJson(path) {
    const response = await fetch(path, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return response.json();
  }

  async function loadFactories() {
    const entries = [];
    const errors = [];
    for (const [type, source] of Object.entries(FACTORY_SOURCES)) {
      try {
        const payload = await fetchJson(source.path);
        if (!Array.isArray(payload)) {
          throw new Error("ожидался список фабрик");
        }
        payload.forEach((item) => {
          if (!item || typeof item !== "object") {
            return;
          }
          const factoryId = item.id ?? item.ID ?? item.factory_id;
          if (!factoryId) {
            return;
          }
          const name =
            item.name ??
            item.Name ??
            item.factory_name ??
            item.title ??
            "";
          const count =
            item.item_count ??
            item.ItemCount ??
            item.total ??
            item.count ??
            null;
          entries.push({
            type,
            id: String(factoryId),
            name: name ? String(name) : "",
            itemCount:
              typeof count === "number"
                ? count
                : typeof count === "string" && count.trim()
                ? Number.parseInt(count, 10)
                : null,
          });
        });
      } catch (error) {
        console.error("Не удалось загрузить фабрики:", error);
        errors.push(TYPE_LABELS[type] || type);
      }
    }
    state.loadErrors = errors;
    state.factories = entries.sort((a, b) => {
      if (a.type !== b.type) {
        return a.type.localeCompare(b.type);
      }
      const nameCompare = (a.name || "").localeCompare(b.name || "", "ru", {
        sensitivity: "base",
        numeric: true,
      });
      if (nameCompare !== 0) {
        return nameCompare;
      }
      return a.id.localeCompare(b.id, "ru", { numeric: true });
    });
  }

  async function loadIgnoreLists() {
    const next = new Set();
    for (const [type, path] of Object.entries(IGNORE_SOURCES)) {
      try {
        const payload = await fetchJson(path);
        if (!Array.isArray(payload)) {
          continue;
        }
        payload.forEach((item) => {
          if (item && typeof item === "object") {
            const id = item.id ?? item.ID ?? null;
            if (id) {
              next.add(keyFor(type, String(id)));
            }
          }
        });
      } catch (error) {
        console.warn("Не удалось прочитать Ignore:", error);
      }
    }
    state.ignoreSet = next;
  }

  function renderOverview() {
    if (!(overviewGrid instanceof HTMLElement)) {
      return;
    }
    overviewGrid.innerHTML = "";
    if (!state.factories.length) {
      const card = document.createElement("div");
      card.className = "overview-card overview-card--empty";
      const message = state.loadErrors.length
        ? `Не удалось загрузить: ${state.loadErrors.join(", ")}`
        : "Фабрики не найдены. Запустите getFabrik.py.";
      card.innerHTML = `<p>${message}</p>`;
      overviewGrid.append(card);
      return;
    }

    const grouped = new Map();
    state.factories.forEach((factory) => {
      const list = grouped.get(factory.type) || [];
      list.push(factory);
      grouped.set(factory.type, list);
    });

    grouped.forEach((list, type) => {
      const ignoredCount = list.filter((factory) =>
        state.ignoreSet.has(keyFor(factory.type, factory.id))
      ).length;

      const card = document.createElement("article");
      card.className = "overview-card";

      const title = document.createElement("h3");
      title.textContent = TYPE_LABELS[type] || type;

      const stats = document.createElement("p");
      stats.className = "overview-card__stats";
      stats.innerHTML = `Всего: <strong>${list.length}</strong> · В Ignore: <strong>${ignoredCount}</strong>`;

      const previewList = document.createElement("ul");
      previewList.className = "overview-card__list";
      list.slice(0, 6).forEach((factory) => {
        const item = document.createElement("li");
        const marker = state.ignoreSet.has(keyFor(factory.type, factory.id))
          ? '<span class="overview-card__tag">Ignore</span>'
          : "";
        item.innerHTML = `${marker}${factory.name || "<без названия>"} <span class="overview-card__meta">(${factory.id})</span>`;
        previewList.append(item);
      });
      if (list.length > 6) {
        const more = document.createElement("li");
        more.className = "overview-card__more";
        more.textContent = `… и ещё ${list.length - 6}`;
        previewList.append(more);
      }

      const actions = document.createElement("div");
      actions.className = "overview-card__actions";
      const button = document.createElement("button");
      button.type = "button";
      button.className = "secondary-btn";
      button.textContent = "Выбрать фабрики";
      button.dataset.type = type;
      actions.append(button);

      card.append(title, stats, previewList, actions);
      overviewGrid.append(card);
    });
  }

  function isSelected(type, id) {
    return state.selections.some(
      (entry) => entry.type === type && entry.id === id
    );
  }

  function addSelection(factory) {
    if (isSelected(factory.type, factory.id)) {
      return false;
    }
    state.selections.push({
      type: factory.type,
      id: factory.id,
      name: factory.name,
      itemCount: factory.itemCount ?? null,
      alreadyIgnored: state.ignoreSet.has(keyFor(factory.type, factory.id)),
    });
    return true;
  }

  function removeSelection(type, id) {
    const index = state.selections.findIndex(
      (entry) => entry.type === type && entry.id === id
    );
    if (index === -1) {
      return false;
    }
    state.selections.splice(index, 1);
    return true;
  }

  function renderSelections() {
    if (!selectedList || !emptyState) {
      return;
    }
    selectedList.innerHTML = "";
    if (!state.selections.length) {
      selectedList.hidden = true;
      emptyState.hidden = false;
      return;
    }
    selectedList.hidden = false;
    emptyState.hidden = true;

    const fragment = document.createDocumentFragment();
    state.selections.forEach((entry) => {
      entry.alreadyIgnored = state.ignoreSet.has(keyFor(entry.type, entry.id));
      const item = document.createElement("li");
      item.className = "selection-item";
      if (entry.alreadyIgnored) {
        item.classList.add("selection-item--ignored");
      }

      const info = document.createElement("div");
      info.className = "selection-item__info";

      const title = document.createElement("div");
      title.className = "selection-item__title";
      title.textContent = entry.name || "<без названия>";

      const meta = document.createElement("div");
      meta.className = "selection-item__meta";
      const parts = [TYPE_LABELS[entry.type] || entry.type, `ID: ${entry.id}`];
      if (typeof entry.itemCount === "number") {
        parts.push(`Товаров: ${entry.itemCount}`);
      }
      if (entry.alreadyIgnored) {
        parts.push("уже в Ignore");
      }
      meta.textContent = parts.join(" • ");

      info.append(title, meta);

      const removeButton = document.createElement("button");
      removeButton.type = "button";
      removeButton.className = "selection-remove";
      removeButton.dataset.type = entry.type;
      removeButton.dataset.id = entry.id;
      removeButton.textContent = "Убрать";

      item.append(info, removeButton);
      fragment.append(item);
    });

    selectedList.append(fragment);
  }

  function updateApplyButton() {
    if (!(applyButton instanceof HTMLButtonElement)) {
      return;
    }
    if (state.isApplying) {
      applyButton.disabled = true;
      applyButton.textContent = "Применение…";
    } else {
      applyButton.disabled = state.selections.length === 0;
      applyButton.textContent = "Применить";
    }
  }

  function clearResults() {
    if (resultPanel instanceof HTMLElement) {
      resultPanel.hidden = true;
    }
    if (resultList instanceof HTMLElement) {
      resultList.innerHTML = "";
    }
  }

  function showResults(results) {
    if (!(resultPanel instanceof HTMLElement) || !(resultList instanceof HTMLElement)) {
      return;
    }
    resultList.innerHTML = "";
    if (!results || !results.length) {
      resultPanel.hidden = true;
      return;
    }
    resultPanel.hidden = false;
    const fragment = document.createDocumentFragment();
    results.forEach((entry) => {
      const item = document.createElement("li");
      item.className = "result-panel__item";
      const status = entry.status || "info";
      if (status === "added" || status === "updated") {
        item.classList.add("result-panel__item--ok");
      } else if (status === "exists") {
        item.classList.add("result-panel__item--skip");
      } else if (status === "error") {
        item.classList.add("result-panel__item--error");
      }
      const title = document.createElement("div");
      title.className = "result-panel__title";
      title.textContent =
        entry.name ||
        `${entry.type || ""} ${entry.id || ""}`.trim() ||
        "Неизвестная фабрика";

      const details = document.createElement("div");
      details.className = "result-panel__details";
      const typeLabel = TYPE_LABELS[entry.type] || entry.type || "—";
      let message = entry.message || "";
      if (!message) {
        if (entry.status === "added") {
          message = "Добавлено в Ignore.";
        } else if (entry.status === "updated") {
          message = "Имя обновлено в Ignore.";
        } else if (entry.status === "exists") {
          message = "Запись уже существует.";
        } else {
          message = "Нет подробностей.";
        }
      }
      details.textContent = `${typeLabel} • ID: ${entry.id || "—"} • ${message}`;

      item.append(title, details);
      fragment.append(item);
    });
    resultList.append(fragment);
  }

  function renderPickerList() {
    if (!pickerList) {
      return;
    }
    pickerList.innerHTML = "";
    if (!state.factories.length) {
      const status = document.createElement("div");
      status.className = "factory-picker__status";
      status.textContent = state.loadErrors.length
        ? `Не удалось загрузить: ${state.loadErrors.join(", ")}`
        : "Данные о фабриках не найдены.";
      pickerList.append(status);
      return;
    }

    const query =
      pickerSearch instanceof HTMLInputElement
        ? pickerSearch.value.trim().toLowerCase()
        : "";
    const selectedType =
      pickerType instanceof HTMLSelectElement ? pickerType.value : "all";
    const ignoreFilter =
      pickerIgnoreFilter instanceof HTMLSelectElement
        ? pickerIgnoreFilter.value
        : "all";

    const filtered = state.factories.filter((factory) => {
      if (selectedType !== "all" && factory.type !== selectedType) {
        return false;
      }
      const alreadyIgnored = state.ignoreSet.has(
        keyFor(factory.type, factory.id)
      );
      if (ignoreFilter === "new" && alreadyIgnored) {
        return false;
      }
      if (ignoreFilter === "ignored" && !alreadyIgnored) {
        return false;
      }
      if (!query) {
        return true;
      }
      const lowerName = (factory.name || "").toLowerCase();
      return (
        lowerName.includes(query) ||
        factory.id.toLowerCase().includes(query)
      );
    });

    if (!filtered.length) {
      const empty = document.createElement("div");
      empty.className = "factory-picker__empty";
      empty.textContent = "Ничего не найдено под указанным фильтром.";
      pickerList.append(empty);
      return;
    }

    const fragment = document.createDocumentFragment();
    filtered.forEach((factory) => {
      const item = document.createElement("div");
      item.className = "factory-picker__item";
      if (state.ignoreSet.has(keyFor(factory.type, factory.id))) {
        item.classList.add("factory-picker__item--ignored");
      }

      const info = document.createElement("div");
      info.className = "factory-picker__info";

      const title = document.createElement("div");
      title.className = "factory-picker__title";
      title.textContent = factory.name || "<без названия>";

      const meta = document.createElement("div");
      meta.className = "factory-picker__meta";
      const parts = [TYPE_LABELS[factory.type] || factory.type, `ID: ${factory.id}`];
      if (typeof factory.itemCount === "number") {
        parts.push(`Товаров: ${factory.itemCount}`);
      }
      if (state.ignoreSet.has(keyFor(factory.type, factory.id))) {
        parts.push("уже в Ignore");
      }
      meta.textContent = parts.join(" • ");

      info.append(title, meta);

      const addButton = document.createElement("button");
      addButton.type = "button";
      addButton.className = "factory-picker__add";
      addButton.dataset.type = factory.type;
      addButton.dataset.id = factory.id;
      addButton.dataset.name = factory.name || "";
      if (typeof factory.itemCount === "number") {
        addButton.dataset.count = String(factory.itemCount);
      }
      const alreadySelected = isSelected(factory.type, factory.id);
      if (alreadySelected) {
        addButton.textContent = "Добавлено";
        addButton.disabled = true;
      } else {
        addButton.textContent = state.ignoreSet.has(keyFor(factory.type, factory.id))
          ? "Добавить (Ignore)"
          : "Добавить";
      }

      item.append(info, addButton);
      fragment.append(item);
    });

    pickerList.append(fragment);
  }

  function openPicker(preselectType) {
    if (!(picker instanceof HTMLElement)) {
      return;
    }
    if (pickerType instanceof HTMLSelectElement && preselectType) {
      pickerType.value = preselectType;
    }
    picker.classList.add("is-open");
    picker.setAttribute("aria-hidden", "false");
    document.body.style.overflow = "hidden";
    state.isPickerOpen = true;
    renderPickerList();
    if (pickerSearch instanceof HTMLInputElement) {
      pickerSearch.focus();
      pickerSearch.select();
    }
  }

  function closePicker() {
    if (!(picker instanceof HTMLElement)) {
      return;
    }
    picker.classList.remove("is-open");
    picker.setAttribute("aria-hidden", "true");
    document.body.style.overflow = "";
    state.isPickerOpen = false;
  }

  async function applySelections() {
    if (!state.selections.length || state.isApplying) {
      return;
    }
    state.isApplying = true;
    updateApplyButton();
    clearResults();

    try {
      const includeOverwrite =
        overwriteCheckbox instanceof HTMLInputElement && overwriteCheckbox.checked;
      const payload = {
        overwrite: includeOverwrite,
        selections: state.selections.map((entry) => ({
          type: entry.type,
          id: entry.id,
          name: entry.name,
        })),
      };
      const response = await fetch("/api/add-ignore", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Запрос вернул статус ${response.status}`);
      }
      const data = await response.json();
      const results = Array.isArray(data.results) ? data.results : [];
      showResults(results);

      if (Array.isArray(data.ignore_keys) && data.ignore_keys.length) {
        state.ignoreSet = new Set(data.ignore_keys);
      } else {
        await loadIgnoreLists();
      }

      const failedKeys = new Set(
        results
          .filter((entry) => entry.status === "error")
          .map((entry) => keyFor(entry.type, entry.id))
      );

      state.selections = state.selections.filter((entry) => {
        if (failedKeys.has(keyFor(entry.type, entry.id))) {
          return true;
        }
        return false;
      });
    } catch (error) {
      console.error("Не удалось применить Ignore:", error);
      showResults([
        {
          status: "error",
          type: "",
          id: "",
          name: "",
          message: error.message || "Неизвестная ошибка",
        },
      ]);
    } finally {
      state.isApplying = false;
      updateApplyButton();
      renderSelections();
      renderOverview();
      renderPickerList();
    }
  }

  async function bootstrap() {
    if (
      !overviewGrid ||
      !(form instanceof HTMLFormElement) ||
      !selectedList ||
      !emptyState ||
      !(openPickerButton instanceof HTMLButtonElement) ||
      !(applyButton instanceof HTMLButtonElement) ||
      !pickerList
    ) {
      console.error("Страница add-ignore загружена не полностью.");
      return;
    }

    await loadFactories();
    await loadIgnoreLists();
    renderOverview();
    renderSelections();
    renderPickerList();
    updateApplyButton();

    openPickerButton.addEventListener("click", () => openPicker());

    overviewGrid.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLButtonElement) || !target.dataset.type) {
        return;
      }
      openPicker(target.dataset.type);
    });

    if (pickerSearch instanceof HTMLInputElement) {
      pickerSearch.addEventListener("input", renderPickerList);
    }
    if (pickerType instanceof HTMLSelectElement) {
      pickerType.addEventListener("change", renderPickerList);
    }
    if (pickerIgnoreFilter instanceof HTMLSelectElement) {
      pickerIgnoreFilter.addEventListener("change", renderPickerList);
    }

    pickerList.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLButtonElement)) {
        return;
      }
      if (!target.classList.contains("factory-picker__add")) {
        return;
      }
      const type = target.dataset.type || "";
      const id = target.dataset.id || "";
      if (!type || !id) {
        return;
      }
      const factory = state.factories.find(
        (item) => item.type === type && item.id === id
      );
      if (!factory) {
        return;
      }
      if (addSelection(factory)) {
        renderSelections();
        updateApplyButton();
        renderPickerList();
      }
    });

    selectedList.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLButtonElement)) {
        return;
      }
      if (!target.classList.contains("selection-remove")) {
        return;
      }
      const type = target.dataset.type || "";
      const id = target.dataset.id || "";
      if (!type || !id) {
        return;
      }
      if (removeSelection(type, id)) {
        renderSelections();
        updateApplyButton();
        renderPickerList();
      }
    });

    applyButton.addEventListener("click", applySelections);

    if (clearResultButton instanceof HTMLButtonElement) {
      clearResultButton.addEventListener("click", clearResults);
    }

    pickerCloseButtons?.forEach((button) => {
      if (button instanceof HTMLButtonElement) {
        button.addEventListener("click", () => closePicker());
      }
    });

    picker?.addEventListener("click", (event) => {
      if (event.target === picker) {
        closePicker();
      }
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        if (state.isPickerOpen) {
          closePicker();
        }
      }
    });
  }

  bootstrap();
})();
