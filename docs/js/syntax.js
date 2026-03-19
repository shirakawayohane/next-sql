// syntax.js — loads markdown sections and renders them into the docs page

(function () {
  "use strict";

  const CONTENT_BASE = "content/syntax/";
  const MANIFEST_URL = CONTENT_BASE + "manifest.json";

  const mainEl = document.getElementById("docs-content");
  const sidebarNav = document.getElementById("sidebar-nav");

  async function loadManifest() {
    const res = await fetch(MANIFEST_URL);
    if (!res.ok) throw new Error("Failed to load manifest");
    return res.json();
  }

  async function loadMarkdown(filename) {
    const res = await fetch(CONTENT_BASE + filename);
    if (!res.ok) throw new Error("Failed to load " + filename);
    return res.text();
  }

  function renderMarkdown(md) {
    // Register nsql as alias for sql in highlight.js
    if (typeof hljs !== "undefined" && !hljs.getLanguage("nsql")) {
      hljs.registerAliases("nsql", { languageName: "sql" });
    }

    var renderer = new marked.Renderer();
    renderer.code = function (obj) {
      var code = typeof obj === "object" ? obj.text : obj;
      var lang = typeof obj === "object" ? obj.lang : arguments[1];
      var highlighted;
      if (lang && hljs.getLanguage(lang)) {
        highlighted = hljs.highlight(code, { language: lang }).value;
      } else {
        highlighted = hljs.highlightAuto(code).value;
      }
      return (
        '<pre><code class="hljs language-' +
        (lang || "") +
        '">' +
        highlighted +
        "</code></pre>"
      );
    };

    return marked.parse(md, {
      gfm: true,
      breaks: false,
      renderer: renderer,
    });
  }

  function buildSidebarLink(section) {
    const li = document.createElement("li");
    const a = document.createElement("a");
    a.href = "#" + section.id;
    a.textContent = section.title;
    a.dataset.id = section.id;
    li.appendChild(a);
    return li;
  }

  function highlightActiveSidebar() {
    const sections = document.querySelectorAll(".md-section");
    const links = sidebarNav.querySelectorAll("a");
    let current = "";

    sections.forEach(function (sec) {
      const rect = sec.getBoundingClientRect();
      if (rect.top <= 120) {
        current = sec.id;
      }
    });

    links.forEach(function (link) {
      link.classList.toggle("active", link.dataset.id === current);
    });
  }

  async function init() {
    try {
      const manifest = await loadManifest();

      // Load all markdown files in parallel
      const contents = await Promise.all(
        manifest.sections.map(function (s) {
          return loadMarkdown(s.file);
        })
      );

      // Clear loading state
      mainEl.innerHTML = "";
      sidebarNav.innerHTML = "";

      manifest.sections.forEach(function (section, i) {
        // Sidebar
        sidebarNav.appendChild(buildSidebarLink(section));

        // Content
        const div = document.createElement("div");
        div.className = "md-section md-content";
        div.id = section.id;
        div.innerHTML = renderMarkdown(contents[i]);
        mainEl.appendChild(div);

        // Divider (except last)
        if (i < manifest.sections.length - 1) {
          const hr = document.createElement("div");
          hr.className = "section-divider";
          mainEl.appendChild(hr);
        }
      });

      // Highlight code blocks that marked didn't catch
      document.querySelectorAll(".md-content pre code").forEach(function (el) {
        if (!el.classList.contains("hljs")) {
          hljs.highlightElement(el);
        }
      });

      // Scroll spy
      window.addEventListener("scroll", highlightActiveSidebar, {
        passive: true,
      });
      highlightActiveSidebar();
    } catch (err) {
      mainEl.innerHTML =
        '<p style="color:#f87171">Failed to load documentation: ' +
        err.message +
        "</p>";
    }
  }

  // Mobile sidebar
  var toggle = document.getElementById("sidebar-toggle");
  var sidebar = document.getElementById("sidebar");
  var overlay = document.getElementById("sidebar-overlay");

  if (toggle && sidebar) {
    toggle.addEventListener("click", function () {
      sidebar.classList.toggle("open");
      if (overlay) overlay.classList.toggle("open");
    });
    if (overlay) {
      overlay.addEventListener("click", function () {
        sidebar.classList.remove("open");
        overlay.classList.remove("open");
      });
    }
    // Close sidebar on link click (mobile)
    sidebarNav.addEventListener("click", function (e) {
      if (e.target.tagName === "A" && window.innerWidth <= 900) {
        sidebar.classList.remove("open");
        if (overlay) overlay.classList.remove("open");
      }
    });
  }

  // Configure marked
  if (typeof marked !== "undefined") {
    marked.setOptions({
      gfm: true,
      breaks: false,
    });
  }

  document.addEventListener("DOMContentLoaded", init);
})();
