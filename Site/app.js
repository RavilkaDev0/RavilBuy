"use strict";

(function () {
  const SAFE_VALUE = /^[0-9A-Za-z_.@%+=:,/\\-]+$/;
  const TOTAL_MAIN_STEPS = 7;
  const DEFAULT_COMMANDS = {
    login: "python Login.py",
    fabrik: "python getFabrik.py",
    items: "python getItems.py",
    exportProducts: "python exportProdukt.py",
    exportLister: "python exportLister.py",
    makeHTML: "python makeHTML.py",
    killFabriks: "python killFabriks.py",
  };
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
  const TYPE_LABELS = Object.fromEntries(
    Object.entries(FACTORY_SOURCES).map(([type, { label }]) => [type, label])
  );
  const COLLATOR = new Intl.Collator("ru", {
    sensitivity: "base",
    usage: "sort",
    numeric: true,
  });

  const killState = {
    selections: [],
  };
  let cachedFactories = [];
  let factoriesLoaded = false;
  let factoriesPromise = null;
  let factoryLoadErrors = [];
  let isFactoryPickerOpen = false;
  let closeFactoryPicker = null;

  const openButtons = document.querySelectorAll(".open-modal");
  const closeButtons = document.querySelectorAll(".modal [data-close]");
  const backdrop = document.querySelector("[data-backdrop]");
  const modals = Array.from(document.querySelectorAll(".modal"));
  let activeModal = null;

  function openModal(id) {
    const modal = document.getElementById(id);
    if (!modal) {
      return;
    }
    closeModal();
    modal.classList.add("is-open");
    modal.setAttribute("aria-hidden", "false");
    if (backdrop) {
      backdrop.classList.add("is-open");
      backdrop.setAttribute("aria-hidden", "false");
    }
    activeModal = modal;
    const firstFocusable = modal.querySelector(
      "button, [href], input, select, textarea"
    );
    if (firstFocusable && typeof firstFocusable.focus === "function") {
      setTimeout(() => firstFocusable.focus(), 50);
    }
  }

  function closeModal() {
    if (!activeModal) {
      return;
    }
    activeModal.classList.remove("is-open");
    activeModal.setAttribute("aria-hidden", "true");
    if (backdrop) {
      backdrop.classList.remove("is-open");
      backdrop.setAttribute("aria-hidden", "true");
    }
    activeModal = null;
  }

  openButtons.forEach((button) => {
    if (button.tagName.toLowerCase() === "a") {
      return;
    }
    button.addEventListener("click", () => {
      const targetId = button.dataset.modal;
      if (targetId) {
        openModal(targetId);
      }
    });
  });

  closeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      closeModal();
    });
  });

  if (backdrop) {
    backdrop.addEventListener("click", () => {
      closeModal();
    });
  }

  modals.forEach((modal) => {
    modal.addEventListener("click", (event) => {
      if (event.target === modal) {
        closeModal();
      }
    });
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }
    if (isFactoryPickerOpen && typeof closeFactoryPicker === "function") {
      closeFactoryPicker();
      return;
    }
    if (activeModal) {
      closeModal();
    }
  });

  function shellQuote(rawPart) {
    if (rawPart === undefined || rawPart === null) {
      return "";
    }
    const part = String(rawPart);
    if (!part) {
      return "";
    }
    if (part.startsWith("-")) {
      return part;
    }
    if (SAFE_VALUE.test(part)) {
      return part;
    }
    const escaped = part.replace(/(["\\$`])/g, "\\$1");
    return `"${escaped}"`;
  }

  function joinCommand(parts) {
    return parts
      .map((part) => (typeof part === "number" ? String(part) : part))
      .filter((part) => part !== undefined && part !== null && part !== "")
      .map(shellQuote)
      .join(" ");
  }

  function getFieldValue(form, name) {
    const field = form.elements.namedItem(name);
    if (!field) {
      return "";
    }
    if (field instanceof HTMLInputElement) {
      return field.value.trim();
    }
    if (field instanceof HTMLTextAreaElement) {
      return field.value.trim();
    }
    if (field instanceof HTMLSelectElement) {
      return field.value.trim();
    }
    return "";
  }

  function getCheckedValues(form, name) {
    return Array.from(form.querySelectorAll(`input[name="${name}"]:checked`)).map(
      (input) => input.value
    );
  }

  function parseList(value) {
    if (!value) {
      return [];
    }
    return value
      .split(/[\r\n,;]+/)
      .map((item) => item.trim())
      .filter(Boolean);
  }

  function parseInteger(value) {
    if (!value) {
      return null;
    }
    const parsed = Number.parseInt(value, 10);
    return Number.isNaN(parsed) ? null : parsed;
  }

  function escapeDoubleQuotes(value) {
    return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  }

  function buildKillAddCommand(type, id, name) {
    if (!id) {
      return "";
    }
    const safeType = escapeDoubleQuotes(type || "");
    const safeId = escapeDoubleQuotes(id);
    const safeName = escapeDoubleQuotes(name || "");
    const snippet = `from killFabriks import add_kill_fabriks; add_kill_fabriks("${safeType}", "${safeId}", "${safeName}")`;
    return `python -c "${snippet}"`;
  }

  function normalizeFactoryRecord(type, data) {
    if (!data || typeof data !== "object") {
      return null;
    }
    const rawId = data.id ?? data.ID ?? data.factory_id ?? "";
    if (!rawId) {
      return null;
    }
    const record = {
      type,
      id: String(rawId),
      name: data.name ? String(data.name) : "",
    };
    const countValue =
      data.item_count ??
      data.itemCount ??
      data.count ??
      data.total ??
      data.itemTotal ??
      null;
    if (typeof countValue === "number") {
      record.itemCount = countValue;
    } else if (typeof countValue === "string" && countValue.trim()) {
      const parsed = Number.parseInt(countValue, 10);
      if (!Number.isNaN(parsed)) {
        record.itemCount = parsed;
      }
    }
    return record;
  }

  async function loadAllFactories() {
    if (factoriesLoaded) {
      return cachedFactories;
    }
    if (!factoriesPromise) {
      factoryLoadErrors = [];
      factoriesPromise = (async () => {
        const collected = [];
        for (const [type, source] of Object.entries(FACTORY_SOURCES)) {
          try {
            const response = await fetch(source.path, { cache: "no-cache" });
            if (!response.ok) {
              throw new Error(`HTTP ${response.status}`);
            }
            const payload = await response.json();
            if (Array.isArray(payload)) {
              payload.forEach((entry) => {
                const record = normalizeFactoryRecord(type, entry);
                if (record) {
                  collected.push(record);
                }
              });
            } else {
              throw new Error("Unexpected JSON формат");
            }
          } catch (error) {
            console.error(`Не удалось загрузить ${type}:`, error);
            factoryLoadErrors.push(TYPE_LABELS[type] || type);
          }
        }
        collected.sort((a, b) => {
          if (a.type !== b.type) {
            return COLLATOR.compare(a.type, b.type);
          }
          const nameCompare = COLLATOR.compare(
            a.name || "",
            b.name || ""
          );
          if (nameCompare !== 0) {
            return nameCompare;
          }
          return COLLATOR.compare(a.id, b.id);
        });
        cachedFactories = collected;
        factoriesLoaded = true;
        return cachedFactories;
      })().catch((error) => {
        factoriesPromise = null;
        throw error;
      });
    }
    return factoriesPromise;
  }

  function isFactorySelected(type, id) {
    return killState.selections.some(
      (entry) => entry.type === type && entry.id === id
    );
  }

  function addFactorySelection(factory) {
    if (!factory || !factory.id || !factory.type) {
      return false;
    }
    if (isFactorySelected(factory.type, factory.id)) {
      return false;
    }
    killState.selections.push({
      type: factory.type,
      id: factory.id,
      name: factory.name || "",
      count:
        typeof factory.itemCount === "number" ? factory.itemCount : null,
    });
    return true;
  }

  function removeFactorySelection(type, id) {
    const index = killState.selections.findIndex(
      (entry) => entry.type === type && entry.id === id
    );
    if (index === -1) {
      return false;
    }
    killState.selections.splice(index, 1);
    return true;
  }

  function setupMainRunner() {
    const form = document.querySelector(
      '.command-form[data-script="main"]'
    );
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    form.addEventListener("submit", (event) => event.preventDefault());

    const runButton = form.querySelector("[data-run-main]");
    const statusNode = form.querySelector("[data-main-status]");
    const resultPanel = document.querySelector("[data-main-result]");
    const resultLog = document.querySelector("[data-main-log]");
    const clearButton = document.querySelector("[data-main-clear]");

    if (
      !(runButton instanceof HTMLButtonElement) ||
      !(resultPanel instanceof HTMLElement) ||
      !(resultLog instanceof HTMLElement)
    ) {
      return;
    }

    function setStatus(message, isError = false) {
      if (!(statusNode instanceof HTMLElement)) {
        return;
      }
      statusNode.textContent = message;
      statusNode.classList.toggle("hint--error", isError);
    }

    function setRunning(running) {
      runButton.disabled = running;
      runButton.textContent = running ? "Запуск..." : "Запустить main.py";
    }

    function collectPayload() {
      const steps = getCheckedValues(form, "steps");
      const skip = getCheckedValues(form, "skip");
      const logLevel = getFieldValue(form, "log-level") || "INFO";
      const payload = {
        log_level: logLevel,
      };
      if (steps.length > 0 && steps.length < TOTAL_MAIN_STEPS) {
        payload.steps = steps;
      }
      if (skip.length > 0) {
        payload.skip = skip;
      }
      return payload;
    }

    function renderResult(data) {
      resultPanel.hidden = false;
      const success = typeof data.returncode === "number" && data.returncode === 0;
      resultPanel.classList.toggle("result-panel--error", !success);
      const stdout = (data.stdout || "").trim();
      const stderr = (data.stderr || "").trim();
      const command = data.command || "";
      const lines = [
        `Код возврата: ${data.returncode}`,
        command ? `Команда: ${command}` : "",
        "",
        "[STDOUT]",
        stdout || "<пусто>",
        "",
        "[STDERR]",
        stderr || "<пусто>",
      ].filter(Boolean);
      resultLog.textContent = lines.join("\n");
    }

    function renderError(message) {
      resultPanel.hidden = false;
      resultPanel.classList.add("result-panel--error");
      resultLog.textContent = `[ERROR]\n${message}`;
    }

    runButton.addEventListener("click", async () => {
      setRunning(true);
      setStatus("Запуск...", false);
      resultPanel.hidden = true;
      resultPanel.classList.remove("result-panel--error");
      try {
        const response = await fetch("/api/run-main", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(collectPayload()),
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok) {
          const message = data.error || `Ошибка запуска (HTTP ${response.status})`;
          throw new Error(message);
        }
        renderResult(data);
        const success = typeof data.returncode === "number" && data.returncode === 0;
        setStatus(
          success
            ? "Запуск завершён успешно."
            : `Завершено с кодом ${data.returncode}.`,
          !success
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        renderError(message);
        setStatus(message, true);
      } finally {
        setRunning(false);
      }
    });

    if (clearButton instanceof HTMLButtonElement) {
      clearButton.addEventListener("click", () => {
        resultPanel.hidden = true;
        resultPanel.classList.remove("result-panel--error");
        resultLog.textContent = "";
        setStatus("Готово к запуску.", false);
      });
    }
  }

  const commandBuilders = {
    login(form) {
      const parts = ["python", "Login.py"];
      const accounts = getCheckedValues(form, "account");
      accounts.forEach((acc) => {
        parts.push("--account", acc);
      });
      const verbose = form.elements.namedItem("verbose");
      if (verbose instanceof HTMLInputElement && verbose.checked) {
        parts.push("--verbose");
      }
      return joinCommand(parts);
    },

    fabrik(form) {
      const parts = ["python", "getFabrik.py"];
      const account = getFieldValue(form, "account");
      if (account) {
        parts.push("--account", account);
      }
      const target = getFieldValue(form, "target") || "all";
      if (target && target !== "all") {
        parts.push("--target", target);
      }
      const verbose = form.elements.namedItem("verbose");
      if (verbose instanceof HTMLInputElement && verbose.checked) {
        parts.push("--verbose");
      }
      return joinCommand(parts);
    },

    items(form) {
      const parts = ["python", "getItems.py"];
      const account = getFieldValue(form, "account");
      if (account) {
        parts.push("--account", account);
      }
      const dataset = getFieldValue(form, "dataset") || "all";
      if (dataset && dataset !== "all") {
        parts.push("--dataset", dataset);
      }
      const limit = parseInteger(getFieldValue(form, "limit"));
      if (limit) {
        parts.push("--limit", String(limit));
      }
      const verbose = form.elements.namedItem("verbose");
      if (verbose instanceof HTMLInputElement && verbose.checked) {
        parts.push("--verbose");
      }
      return joinCommand(parts);
    },

    exportProducts(form) {
      const parts = ["python", "exportProdukt.py"];
      const accounts = getCheckedValues(form, "account");
      accounts.forEach((acc) => {
        parts.push("--account", acc);
      });
      const factoryIds = parseList(getFieldValue(form, "factory-ids"));
      factoryIds.forEach((id) => {
        parts.push("--factory-id", id);
      });
      const factoryNames = parseList(getFieldValue(form, "factory-names"));
      factoryNames.forEach((name) => {
        parts.push("--factory-name", name);
      });
      const limit = parseInteger(getFieldValue(form, "limit"));
      if (limit) {
        parts.push("--limit", String(limit));
      }
      const outputDir = getFieldValue(form, "output-dir");
      if (outputDir && outputDir !== "CSVDATA") {
        parts.push("--output-dir", outputDir);
      }
      const definitionId = getFieldValue(form, "definition-id");
      if (definitionId) {
        parts.push("--definition-id", definitionId);
      }
      const exportFormatId = getFieldValue(form, "export-format-id");
      if (exportFormatId) {
        parts.push("--export-format-id", exportFormatId);
      }
      const expprod = getFieldValue(form, "expprod") || "3";
      if (expprod && expprod !== "3") {
        parts.push("--expprod", expprod);
      }
      const exportEncoding = getFieldValue(form, "export-encoding") || "1";
      if (exportEncoding && exportEncoding !== "1") {
        parts.push("--export-encoding", exportEncoding);
      }
      const saveExportEncoding =
        getFieldValue(form, "save-export-encoding") || "1";
      if (saveExportEncoding && saveExportEncoding !== "1") {
        parts.push("--save-export-encoding", saveExportEncoding);
      }
      const skipExisting = form.elements.namedItem("skip-existing");
      if (skipExisting instanceof HTMLInputElement && skipExisting.checked) {
        parts.push("--skip-existing");
      }
      const dryRun = form.elements.namedItem("dry-run");
      if (dryRun instanceof HTMLInputElement && dryRun.checked) {
        parts.push("--dry-run");
      }
      const verbose = form.elements.namedItem("verbose");
      if (verbose instanceof HTMLInputElement && verbose.checked) {
        parts.push("--verbose");
      }
      return joinCommand(parts);
    },

    exportLister(form) {
      const parts = ["python", "exportLister.py"];
      const accounts = getCheckedValues(form, "account");
      accounts.forEach((acc) => {
        parts.push("--account", acc);
      });
      const factoryIds = parseList(getFieldValue(form, "factory-ids"));
      factoryIds.forEach((id) => {
        parts.push("--factory-id", id);
      });
      const factoryNames = parseList(getFieldValue(form, "factory-names"));
      factoryNames.forEach((name) => {
        parts.push("--factory-name", name);
      });
      const limit = parseInteger(getFieldValue(form, "limit"));
      if (limit) {
        parts.push("--limit", String(limit));
      }
      const outputDir = getFieldValue(form, "output-dir");
      if (outputDir && outputDir !== "CSVDATA") {
        parts.push("--output-dir", outputDir);
      }
      const definitionId = getFieldValue(form, "definition-id");
      if (definitionId) {
        parts.push("--definition-id", definitionId);
      }
      const exportFormatId = getFieldValue(form, "export-format-id");
      if (exportFormatId) {
        parts.push("--export-format-id", exportFormatId);
      }
      const expprod = getFieldValue(form, "expprod") || "3";
      if (expprod && expprod !== "3") {
        parts.push("--expprod", expprod);
      }
      const exportEncoding = getFieldValue(form, "export-encoding") || "1";
      if (exportEncoding && exportEncoding !== "1") {
        parts.push("--export-encoding", exportEncoding);
      }
      const skipExisting = form.elements.namedItem("skip-existing");
      if (skipExisting instanceof HTMLInputElement && skipExisting.checked) {
        parts.push("--skip-existing");
      }
      const dryRun = form.elements.namedItem("dry-run");
      if (dryRun instanceof HTMLInputElement && dryRun.checked) {
        parts.push("--dry-run");
      }
      const verbose = form.elements.namedItem("verbose");
      if (verbose instanceof HTMLInputElement && verbose.checked) {
        parts.push("--verbose");
      }
      return joinCommand(parts);
    },

    makeHTML(form) {
      const parts = ["python", "makeHTML.py"];
      const accounts = getCheckedValues(form, "account");
      if (accounts.length > 0) {
        parts.push("--account");
        parts.push(...accounts);
      }
      const inputBase = getFieldValue(form, "input-base");
      if (inputBase && inputBase !== "CSVDATA") {
        parts.push("--input-base", inputBase);
      }
      const outputBase = getFieldValue(form, "output-base");
      if (outputBase && outputBase !== "readyhtml") {
        parts.push("--output-base", outputBase);
      }
      const delimiter = getFieldValue(form, "delimiter") || ";";
      if (delimiter && delimiter !== ";") {
        parts.push("--delimiter", delimiter);
      }
      const limit = parseInteger(getFieldValue(form, "limit"));
      if (limit) {
        parts.push("--limit", String(limit));
      }
      const noOverwrite = form.elements.namedItem("no-overwrite");
      if (noOverwrite instanceof HTMLInputElement && noOverwrite.checked) {
        parts.push("--no-overwrite");
      }
      const verbose = form.elements.namedItem("verbose");
      if (verbose instanceof HTMLInputElement && verbose.checked) {
        parts.push("--verbose");
      }
      return joinCommand(parts);
    },

    killFabriks(form) {
      const commands = [];
      const runCleanup = form.elements.namedItem("run-cleanup");
      if (runCleanup instanceof HTMLInputElement && runCleanup.checked) {
        commands.push(DEFAULT_COMMANDS.killFabriks);
      }
      killState.selections.forEach(({ type, id, name }) => {
        const command = buildKillAddCommand(type, id, name);
        if (command) {
          commands.push(command);
        }
      });
      if (commands.length === 0) {
        return DEFAULT_COMMANDS.killFabriks;
      }
      return commands.join("\n");
    },
  };

  function setupKillFabriksUI() {
    const form = document.querySelector(
      '.command-form[data-script="killFabriks"]'
    );
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    const selectedList = form.querySelector("[data-selected-list]");
    const emptyState = form.querySelector("[data-empty-state]");
    const openPickerButton = form.querySelector("[data-open-picker]");
    if (
      !(selectedList instanceof HTMLElement) ||
      !(emptyState instanceof HTMLElement) ||
      !(openPickerButton instanceof HTMLButtonElement)
    ) {
      return;
    }

    const picker = document.getElementById("factory-picker");
    if (!(picker instanceof HTMLElement)) {
      return;
    }
    const pickerList = picker.querySelector("[data-picker-list]");
    const searchInput = picker.querySelector("[data-picker-search]");
    const typeSelect = picker.querySelector("[data-picker-type]");
    const closeButtons = picker.querySelectorAll("[data-picker-close]");
    if (!(pickerList instanceof HTMLElement)) {
      return;
    }

    function renderSelected() {
      selectedList.innerHTML = "";
      if (killState.selections.length === 0) {
        selectedList.hidden = true;
        emptyState.hidden = false;
        return;
      }
      selectedList.hidden = false;
      emptyState.hidden = true;
      const fragment = document.createDocumentFragment();
      killState.selections.forEach(({ type, id, name, count }) => {
        const item = document.createElement("li");
        item.className = "selection-item";

        const info = document.createElement("div");
        info.className = "selection-item__info";

        const title = document.createElement("div");
        title.className = "selection-item__title";
        title.textContent = name || "<без названия>";

        const meta = document.createElement("div");
        meta.className = "selection-item__meta";
        const pieces = [TYPE_LABELS[type] || type, `ID: ${id}`];
        if (typeof count === "number") {
          pieces.push(`Товаров: ${count}`);
        }
        meta.textContent = pieces.join(" • ");

        info.append(title, meta);

        const removeButton = document.createElement("button");
        removeButton.type = "button";
        removeButton.className = "selection-remove";
        removeButton.dataset.type = type;
        removeButton.dataset.id = id;
        removeButton.textContent = "Убрать";

        item.append(info, removeButton);
        fragment.append(item);
      });
      selectedList.append(fragment);
    }

    function renderPickerList() {
      pickerList.innerHTML = "";
      const query =
        searchInput instanceof HTMLInputElement
          ? searchInput.value.trim().toLowerCase()
          : "";
      const selectedType =
        typeSelect instanceof HTMLSelectElement
          ? typeSelect.value
          : "all";
      if (!factoriesLoaded) {
        const loading = document.createElement("div");
        loading.className = "factory-picker__status";
        loading.textContent = "Загрузка списка...";
        pickerList.append(loading);
        return;
      }

      if (cachedFactories.length === 0) {
        const message = document.createElement("div");
        message.className = "factory-picker__status";
        if (factoryLoadErrors.length > 0) {
          message.textContent = `Не удалось загрузить: ${factoryLoadErrors.join(
            ", "
          )}. Проверьте запуск getFabrik.py.`;
        } else {
          message.textContent =
            "Данные о фабриках не найдены. Выполните скрипт getFabrik.py.";
        }
        pickerList.append(message);
        return;
      }

      const normalizedType =
        selectedType && selectedType !== "all" ? selectedType : null;
      const filtered = cachedFactories.filter((factory) => {
        if (normalizedType && factory.type !== normalizedType) {
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

      if (factoryLoadErrors.length > 0) {
        const warn = document.createElement("div");
        warn.className = "factory-picker__status";
        warn.style.color = "#fca5a5";
        warn.textContent = `Не удалось загрузить: ${factoryLoadErrors.join(
          ", "
        )}.`;
        pickerList.append(warn);
      }

      if (filtered.length === 0) {
        const empty = document.createElement("div");
        empty.className = "factory-picker__empty";
        empty.textContent = query
          ? "Ничего не найдено по указанному фильтру."
          : "Подходящие фабрики не найдены.";
        pickerList.append(empty);
        return;
      }

      const fragment = document.createDocumentFragment();
      filtered.forEach((factory) => {
        const item = document.createElement("div");
        item.className = "factory-picker__item";

        const info = document.createElement("div");
        info.className = "factory-picker__info";

        const title = document.createElement("div");
        title.className = "factory-picker__title";
        title.textContent = factory.name || "<без названия>";

        const meta = document.createElement("div");
        meta.className = "factory-picker__meta";
        const details = [TYPE_LABELS[factory.type] || factory.type];
        details.push(`ID: ${factory.id}`);
        if (typeof factory.itemCount === "number") {
          details.push(`Товаров: ${factory.itemCount}`);
        }
        meta.textContent = details.join(" • ");

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
        const alreadySelected = isFactorySelected(
          factory.type,
          factory.id
        );
        addButton.textContent = alreadySelected ? "Добавлено" : "Добавить";
        addButton.disabled = alreadySelected;

        item.append(info, addButton);
        fragment.append(item);
      });

      pickerList.append(fragment);
    }

    function updateCommandPreview() {
      if (typeof form.__updateCommand === "function") {
        form.__updateCommand();
      }
    }

    function openPicker() {
      picker.classList.add("is-open");
      picker.setAttribute("aria-hidden", "false");
      document.body.style.overflow = "hidden";
      isFactoryPickerOpen = true;
      pickerList.innerHTML =
        '<div class="factory-picker__status">Загрузка списка...</div>';
      loadAllFactories()
        .catch(() => {
          pickerList.innerHTML =
            '<div class="factory-picker__status">Не удалось загрузить список фабрик. Проверьте наличие файлов в папке Fabriks.</div>';
        })
        .finally(() => {
          renderPickerList();
          if (searchInput instanceof HTMLInputElement) {
            searchInput.focus();
            searchInput.select();
          }
        });
    }

    function closePicker() {
      if (!isFactoryPickerOpen) {
        return;
      }
      picker.classList.remove("is-open");
      picker.setAttribute("aria-hidden", "true");
      document.body.style.overflow = "";
      isFactoryPickerOpen = false;
    }

    closeFactoryPicker = closePicker;

    openPickerButton.addEventListener("click", () => {
      openPicker();
    });

    closeButtons.forEach((button) => {
      if (button instanceof HTMLButtonElement) {
        button.addEventListener("click", () => {
          closePicker();
        });
      }
    });

    picker.addEventListener("click", (event) => {
      if (event.target === picker) {
        closePicker();
      }
    });

    if (searchInput instanceof HTMLInputElement) {
      searchInput.addEventListener("input", () => {
        renderPickerList();
      });
    }

    if (typeSelect instanceof HTMLSelectElement) {
      typeSelect.addEventListener("change", () => {
        renderPickerList();
      });
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
      const name = target.dataset.name || "";
      const countValue = target.dataset.count || "";
      const count = countValue ? Number.parseInt(countValue, 10) : null;

      const added = addFactorySelection({
        type,
        id,
        name,
        itemCount: Number.isNaN(count) ? null : count,
      });
      if (added) {
        renderSelected();
        updateCommandPreview();
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
      if (removeFactorySelection(type, id)) {
        renderSelected();
        updateCommandPreview();
        renderPickerList();
      }
    });

    renderSelected();
    updateCommandPreview();
  }

  function copyToClipboard(text, button) {
    if (!text) {
      return;
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard
        .writeText(text)
        .then(() => showCopyFeedback(button, true))
        .catch(() => fallbackCopy(text, button));
    } else {
      fallbackCopy(text, button);
    }
  }

  function fallbackCopy(text, button) {
    const temp = document.createElement("textarea");
    temp.value = text;
    temp.setAttribute("readonly", "");
    temp.style.position = "absolute";
    temp.style.left = "-9999px";
    document.body.appendChild(temp);
    temp.select();
    try {
      document.execCommand("copy");
      showCopyFeedback(button, true);
    } catch (error) {
      showCopyFeedback(button, false);
    } finally {
      document.body.removeChild(temp);
    }
  }

  function showCopyFeedback(button, success) {
    if (!(button instanceof HTMLButtonElement)) {
      return;
    }
    const originalText = button.dataset.originalText || button.textContent || "";
    if (!button.dataset.originalText) {
      button.dataset.originalText = originalText;
    }
    button.textContent = success ? "Скопировано!" : "Ошибка копирования";
    setTimeout(() => {
      button.textContent = button.dataset.originalText || "Скопировать";
    }, 2000);
  }

  const forms = document.querySelectorAll(".command-form");
  forms.forEach((form) => {
    const script = form.dataset.script;
    if (!script || !(script in commandBuilders)) {
      return;
    }
    const outputContainer = document.querySelector(
      `[data-output-for="${script}"]`
    );
    if (!outputContainer) {
      return;
    }
    const outputNode = outputContainer.querySelector(".command-preview__code");
    const copyButton = outputContainer.querySelector(
      `[data-copy-source="${script}"]`
    );

    function updateCommand() {
      let commandText = "";
      try {
        commandText = commandBuilders[script](form);
      } catch (error) {
        commandText = DEFAULT_COMMANDS[script] || "";
        console.error("Ошибка построения команды:", error);
      }
      if (!commandText) {
        commandText = DEFAULT_COMMANDS[script] || "";
      }
      if (outputNode) {
        outputNode.textContent = commandText;
      }
    }

    form.__updateCommand = updateCommand;

    form.addEventListener("submit", (event) => {
      event.preventDefault();
      updateCommand();
    });
    form.addEventListener("input", updateCommand);
    form.addEventListener("change", updateCommand);

    if (copyButton instanceof HTMLButtonElement && outputNode) {
      copyButton.addEventListener("click", () => {
        copyToClipboard(outputNode.textContent || "", copyButton);
      });
    }

    updateCommand();
  });

  setupMainRunner();
  setupKillFabriksUI();
})();
