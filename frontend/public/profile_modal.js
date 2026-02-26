/**
 * profile_modal.js
 * -------------------------------------------------------
 * Reusable detailed profile modal component.
 * Used by both Analytics (bucket modal rows) and
 * Alumni Directory (list row actions).
 * -------------------------------------------------------
 */

// eslint-disable-next-line no-unused-vars
class ProfileDetailModal {
  constructor() {
    this.overlay = null;
    this.triggerEl = null;
    this._boundKeydown = this._onKeydown.bind(this);
    this._create();
  }

  // ---- DOM creation ----
  _create() {
    if (document.getElementById('profileDetailOverlay')) {
      this.overlay = document.getElementById('profileDetailOverlay');
      return;
    }

    const el = document.createElement('div');
    el.id = 'profileDetailOverlay';
    el.className = 'profile-detail-overlay';
    el.setAttribute('role', 'dialog');
    el.setAttribute('aria-modal', 'true');
    el.setAttribute('aria-label', 'Alumni Profile Detail');
    el.innerHTML = `
      <div class="profile-detail-modal">
        <div class="profile-detail-header">
          <h2 id="profileDetailTitle">Alumni Profile</h2>
          <button class="profile-detail-close-btn" id="profileDetailCloseBtn" title="Close" aria-label="Close profile">✕</button>
        </div>
        <div class="profile-detail-body" id="profileDetailBody"></div>
        <div class="profile-detail-footer" id="profileDetailFooter">
          <a id="profileDetailLinkedIn" class="btn-profile-action btn-profile-linkedin" href="#" target="_blank" rel="noopener noreferrer">
            <svg viewBox="0 0 24 24" fill="currentColor" width="16" height="16"><path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/></svg>
            View on LinkedIn
          </a>
          <button id="profileDetailCloseFooter" class="btn-profile-action btn-profile-close">Close</button>
        </div>
      </div>
    `;
    document.body.appendChild(el);
    this.overlay = el;

    // Event listeners
    el.querySelector('#profileDetailCloseBtn').addEventListener('click', () => this.close());
    el.querySelector('#profileDetailCloseFooter').addEventListener('click', () => this.close());
    el.addEventListener('click', (e) => {
      if (e.target === el) this.close();
    });
  }

  // ---- Public: open ----
  open(alumniOrId, triggerElement) {
    this.triggerEl = triggerElement || null;

    // If it's an object with an id, use the data we have
    if (alumniOrId && typeof alumniOrId === 'object') {
      // Check if we have detailed fields already (school, exp2_title, etc.)
      const hasDetail = alumniOrId.school || alumniOrId.exp2_title || alumniOrId.school2;
      if (hasDetail) {
        this._render(alumniOrId);
        this._show();
      } else {
        // We have basic data — show it, then try to fetch full detail
        this._render(alumniOrId);
        this._show();
        if (alumniOrId.id) {
          this._fetchAndRender(alumniOrId.id);
        }
      }
    } else if (typeof alumniOrId === 'number' || (typeof alumniOrId === 'string' && !isNaN(alumniOrId))) {
      // ID only — need to fetch
      this._showLoading();
      this._show();
      this._fetchAndRender(parseInt(alumniOrId, 10));
    }
  }

  // ---- Public: close ----
  close() {
    this.overlay.classList.remove('show');
    document.removeEventListener('keydown', this._boundKeydown);

    // Restore focus
    if (this.triggerEl && typeof this.triggerEl.focus === 'function') {
      this.triggerEl.focus();
    }
    this.triggerEl = null;
  }

  // ---- Private helpers ----
  _show() {
    this.overlay.classList.add('show');
    document.addEventListener('keydown', this._boundKeydown);

    // Focus the close button for accessibility
    const closeBtn = this.overlay.querySelector('#profileDetailCloseBtn');
    if (closeBtn) setTimeout(() => closeBtn.focus(), 50);
  }

  _onKeydown(e) {
    if (e.key === 'Escape') {
      e.stopPropagation();
      this.close();
      return;
    }

    // Simple focus trap: Tab cycles within modal
    if (e.key === 'Tab') {
      const modal = this.overlay.querySelector('.profile-detail-modal');
      const focusable = modal.querySelectorAll('button, a[href], input, select, textarea, [tabindex]:not([tabindex="-1"])');
      if (focusable.length === 0) return;

      const first = focusable[0];
      const last = focusable[focusable.length - 1];

      if (e.shiftKey) {
        if (document.activeElement === first) {
          e.preventDefault();
          last.focus();
        }
      } else {
        if (document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
    }
  }

  _showLoading() {
    const body = this.overlay.querySelector('#profileDetailBody');
    body.innerHTML = `
      <div class="profile-detail-loading">
        <div class="profile-detail-spinner"></div>
        Loading profile…
      </div>
    `;
    this.overlay.querySelector('#profileDetailTitle').textContent = 'Alumni Profile';
    this._hideLinkedIn();
  }

  _showError(msg) {
    const body = this.overlay.querySelector('#profileDetailBody');
    body.innerHTML = `<div class="profile-detail-error">${this._esc(msg || 'Failed to load profile')}</div>`;
  }

  async _fetchAndRender(id) {
    try {
      const resp = await fetch(`/api/alumni/${id}`);
      if (!resp.ok) {
        this._showError('Could not load profile details.');
        return;
      }
      const json = await resp.json();
      if (json.success && json.alumni) {
        this._render(json.alumni);
      } else {
        this._showError(json.error || 'Alumni not found.');
      }
    } catch (err) {
      console.error('[ProfileDetailModal] fetch error:', err);
      this._showError('Network error loading profile.');
    }
  }

  // ---- Render ----
  _render(data) {
    const body = this.overlay.querySelector('#profileDetailBody');
    const titleEl = this.overlay.querySelector('#profileDetailTitle');
    const linkedinBtn = this.overlay.querySelector('#profileDetailLinkedIn');

    const name = this._safe(data.name);
    titleEl.textContent = name || 'Alumni Profile';

    // LinkedIn button
    const linkedinUrl = data.linkedin || data.linkedin_url;
    if (linkedinUrl) {
      linkedinBtn.href = linkedinUrl;
      linkedinBtn.style.display = 'inline-flex';
    } else {
      this._hideLinkedIn();
    }

    // Build sections
    let html = '';

    // ---- EDUCATION SECTION ----
    html += this._buildEducationSection(data);

    // ---- EXPERIENCE SECTION ----
    html += this._buildExperienceSection(data);

    // ---- HEADLINE ----
    const headline = this._safe(data.headline);
    if (headline) {
      const uniqueId = 'hl_' + Date.now();
      html += `
        <div class="profile-detail-section">
          <div class="profile-detail-section-title">Headline</div>
          <div class="profile-detail-field">
            <div class="profile-detail-value profile-detail-headline" id="${uniqueId}">${this._esc(headline)}</div>
            <button class="profile-detail-toggle" id="${uniqueId}_toggle" onclick="window._profileModalToggleHeadline('${uniqueId}')">Show more</button>
          </div>
        </div>
      `;
    }

    // ---- LAST UPDATED ----
    const updatedAt = this._safe(data.updated_at);
    if (updatedAt) {
      let displayDate = updatedAt;
      try {
        const d = new Date(updatedAt);
        if (!isNaN(d.getTime())) {
          displayDate = d.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
        }
      } catch (_) { /* use raw string */ }

      html += `
        <div class="profile-detail-section">
          <div class="profile-detail-section-title">Last Updated</div>
          <div class="profile-detail-field">
            <div class="profile-detail-value">${this._esc(displayDate)}</div>
          </div>
        </div>
      `;
    }

    body.innerHTML = html;

    // Check headline overflow after render
    requestAnimationFrame(() => {
      const hlEl = body.querySelector('.profile-detail-headline');
      const toggleBtn = body.querySelector('.profile-detail-toggle');
      if (hlEl && toggleBtn) {
        if (hlEl.scrollHeight > hlEl.clientHeight + 2) {
          toggleBtn.classList.add('visible');
        }
      }
    });
  }

  // ---- Education builder ----
  _buildEducationSection(data) {
    const entries = [];

    // Education 1 (UNT primary)
    const edu1 = this._formatEducation(
      data.school || 'University of North Texas',
      data.degree || data.full_degree,
      data.major,
      data.school_start_date,
      data.grad_year
    );
    if (edu1) entries.push({ label: 'Education', value: edu1 });

    // Education 2
    if (this._safeEdu(data.school2) || this._safeEdu(data.degree2) || this._safeEdu(data.major2)) {
      const edu2 = this._formatEducation(data.school2, data.degree2, data.major2);
      if (edu2) entries.push({ label: 'Education 2', value: edu2 });
    }

    // Education 3
    if (this._safeEdu(data.school3) || this._safeEdu(data.degree3) || this._safeEdu(data.major3)) {
      const edu3 = this._formatEducation(data.school3, data.degree3, data.major3);
      if (edu3) entries.push({ label: 'Education 3', value: edu3 });
    }

    if (entries.length === 0) return '';

    let html = '<div class="profile-detail-section"><div class="profile-detail-section-title">🎓 Education</div>';
    for (const entry of entries) {
      html += `
        <div class="profile-detail-field">
          <div class="profile-detail-label">${this._esc(entry.label)}</div>
          <div class="profile-detail-value">${this._esc(entry.value)}</div>
        </div>
      `;
    }
    html += '</div>';
    return html;
  }

  _formatEducation(school, degree, major, startDate, endDate) {
    const s = this._safeEdu(school);
    const d = this._safeEdu(degree);
    const m = this._safeEdu(major);

    if (!s && !d && !m) return '';

    let line = s || '';

    // Build degree part — e.g. "B.S. in Computer Science"
    let degreePart = '';
    if (d && m) {
      degreePart = `${d} in ${m}`;
    } else if (d) {
      degreePart = d;
    } else if (m) {
      degreePart = m;
    }

    if (line && degreePart) {
      line += ` - ${degreePart}`;
    } else if (degreePart) {
      line = degreePart;
    }

    // Dates
    const start = this._safe(startDate);
    const end = this._safe(endDate);
    if (start || end) {
      const datePart = start && end ? `${start} - ${end}` : (start || end);
      line += ` (${datePart})`;
    }

    return line;
  }

  // ---- Experience builder ----
  _buildExperienceSection(data) {
    const entries = [];

    // Experience 1
    const exp1 = this._formatExperience(
      data.company,
      data.current_job_title || data.role,
      data.job_start_date,
      data.job_end_date
    );
    if (exp1) entries.push({ label: 'Experience', value: exp1 });

    // Experience 2
    if (this._safe(data.exp2_title) || this._safe(data.exp2_company)) {
      const exp2 = this._formatExperienceFromDates(data.exp2_company, data.exp2_title, data.exp2_dates);
      if (exp2) entries.push({ label: 'Experience 2', value: exp2 });
    }

    // Experience 3
    if (this._safe(data.exp3_title) || this._safe(data.exp3_company)) {
      const exp3 = this._formatExperienceFromDates(data.exp3_company, data.exp3_title, data.exp3_dates);
      if (exp3) entries.push({ label: 'Experience 3', value: exp3 });
    }

    if (entries.length === 0) return '';

    let html = '<div class="profile-detail-section"><div class="profile-detail-section-title">💼 Experience</div>';
    for (const entry of entries) {
      html += `
        <div class="profile-detail-field">
          <div class="profile-detail-label">${this._esc(entry.label)}</div>
          <div class="profile-detail-value">${this._esc(entry.value)}</div>
        </div>
      `;
    }
    html += '</div>';
    return html;
  }

  _formatExperience(company, title, startDate, endDate) {
    const c = this._safe(company);
    const t = this._safe(title);

    if (!c && !t) return '';

    let line = '';
    if (c && t) {
      line = `${c} - ${t}`;
    } else {
      line = c || t;
    }

    const start = this._safe(startDate);
    let end = this._safe(endDate);

    // Detect "Present" / "current" markers
    if (end) {
      const lowerEnd = end.toLowerCase();
      if (lowerEnd === 'present' || lowerEnd === 'current' || lowerEnd === 'now') {
        end = 'Present';
      }
    }

    if (start || end) {
      const datePart = start && end ? `${start} - ${end}` : (start || end);
      line += ` (${datePart})`;
    }

    return line;
  }

  _formatExperienceFromDates(company, title, datesStr) {
    const c = this._safe(company);
    const t = this._safe(title);

    if (!c && !t) return '';

    let line = '';
    if (c && t) {
      line = `${c} - ${t}`;
    } else {
      line = c || t;
    }

    const dates = this._safe(datesStr);
    if (dates) {
      line += ` (${dates})`;
    }

    return line;
  }

  // ---- Utility ----
  _safe(val) {
    if (val === null || val === undefined) return '';
    const s = String(val).trim();
    return s === 'null' || s === 'undefined' || s === 'None' ? '' : s;
  }

  _safeEdu(val) {
    const s = this._safe(val);
    if (!s) return '';
    const lowered = s.toLowerCase();
    if (['other', 'unknown', 'not found', 'n/a', 'na', 'none', 'null', 'nan'].includes(lowered)) {
      return '';
    }
    return s;
  }

  _esc(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  _hideLinkedIn() {
    const btn = this.overlay.querySelector('#profileDetailLinkedIn');
    if (btn) btn.style.display = 'none';
  }
}

// Global instance accessible from both analytics.js and app.js
const profileDetailModal = new ProfileDetailModal();

// Global toggle function for headline show more/less
window._profileModalToggleHeadline = function(id) {
  const el = document.getElementById(id);
  const btn = document.getElementById(id + '_toggle');
  if (!el || !btn) return;

  if (el.classList.contains('expanded')) {
    el.classList.remove('expanded');
    btn.textContent = 'Show more';
  } else {
    el.classList.add('expanded');
    btn.textContent = 'Show less';
  }
};
