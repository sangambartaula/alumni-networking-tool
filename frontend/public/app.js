// app.js
// Approved engineering disciplines (must match backend APPROVED_ENGINEERING_DISCIPLINES)
const APPROVED_ENGINEERING_DISCIPLINES = [
  'Software, Data & AI Engineering',
  'Embedded, Electrical & Hardware Engineering',
  'Mechanical & Energy Engineering',
  'Biomedical Engineering',
  'Materials Science & Manufacturing',
  'Construction & Engineering Management',
];
// Fake alumni data (fallback). Backend will be queried first; if it fails we use this local list.
const fakeAlumni = [
  { id: 1, name: "Sachin Banjade", role: "Software Engineer", company: "Tech Solutions Inc.", class: 2020, location: "Dallas", linkedin: "https://www.linkedin.com/in/sachin-banjade-345339248/" },
  { id: 2, name: "Sangam Bartaula", role: "Data Scientist", company: "Data Insights Co.", class: 2021, location: "Austin", linkedin: "https://www.linkedin.com/in/sangambartaula/" },
  { id: 3, name: "Shrish Acharya", role: "Product Manager", company: "Innovate Labs", class: 2023, location: "Houston", linkedin: "https://www.linkedin.com/in/shrish-acharya-53b46932b/" },
  { id: 4, name: "Niranjan Paudel", role: "Cybersecurity Analyst", company: "SecureNet Systems", class: 2020, location: "Dallas", linkedin: "https://www.linkedin.com/in/niranjan-paudel-14a31a330/" },
  { id: 5, name: "Abishek Lamichhane", role: "Cloud Architect", company: "Global Cloud Services", class: 2022, location: "Remote", linkedin: "https://www.linkedin.com/in/abishek-lamichhane-b21ab6330/" },
];

// State and configuration
let userInteractions = {};
let loadedAlumni = [];
let totalAlumniCount = 0;
let isLoadingAlumni = false;
let filtersInitialized = false;
let bookmarkedTotalCount = 0;
let sortDirection = 'desc';
let activeQueryState = null;
let activeRequestToken = 0;
// Backend currently caps alumni page size at 500.
const alumniChunkSize = 500;

const listContainer = document.getElementById('list');
const count = document.getElementById('count');

// ===== STATS BANNER UPDATE FUNCTION =====
function updateStatsBanner(alumniData) {
  const safeAlumni = Array.isArray(alumniData) ? alumniData : [];
  const totalAlumni = Number.isFinite(totalAlumniCount) && totalAlumniCount > 0
    ? totalAlumniCount
    : safeAlumni.length;

  // Calculate unique locations
  const uniqueLocations = new Set(safeAlumni.map(a => a.location).filter(_isMeaningfulLocationValue));
  const locationsCount = uniqueLocations.size;

  // Calculate bookmarked alumni - check interaction_type === 'bookmarked'
  const bookmarkedCount = Number.isFinite(bookmarkedTotalCount)
    ? bookmarkedTotalCount
    : Object.values(userInteractions).filter(interaction => interaction.interaction_type === 'bookmarked').length;

  // Calculate working while studying count
  const wwsCount = safeAlumni.filter(a => a.working_while_studying === true).length;

  // Update DOM elements
  const totalAlumniEl = document.getElementById('totalAlumni');
  const locationsCountEl = document.getElementById('locationsCount');
  const bookmarkedCountEl = document.getElementById('bookmarkedCount');
  const wwsCountEl = document.getElementById('workingWhileStudyingCount');

  if (totalAlumniEl) totalAlumniEl.textContent = totalAlumni;
  if (locationsCountEl) locationsCountEl.textContent = locationsCount;
  if (bookmarkedCountEl) bookmarkedCountEl.textContent = bookmarkedCount;
  if (wwsCountEl) wwsCountEl.textContent = wwsCount;
}

// Helper function to update just the bookmarked count in the banner
function updateBookmarkCount() {
  const bookmarkedCount = Number.isFinite(bookmarkedTotalCount)
    ? bookmarkedTotalCount
    : Object.values(userInteractions).filter(interaction => interaction.interaction_type === 'bookmarked').length;
  const bookmarkedCountEl = document.getElementById('bookmarkedCount');
  if (bookmarkedCountEl) bookmarkedCountEl.textContent = bookmarkedCount;
}

/**
 * Modal for managing alumni notes.
 * Uses an optimistic UI pattern: updates local cache (notesStatusCache) 
 * immediately upon successful save to ensure responsive UI feeding back
 * into the main list's note indicators.
 */
class NotesModal {
  constructor() {
    this.modal = null;
    this.currentAlumniId = null;
    this.currentAlumniName = null;
  }

  create() {
    const modalHTML = `
      <div id="notesOverlay" class="notes-overlay">
        <div class="notes-modal">
          <div class="notes-modal-header">
            <h3 id="notesTitle">Notes for Alumni</h3>
            <button class="notes-close-btn" id="notesCloseBtn">&times;</button>
          </div>
          <textarea id="notesTextarea" class="notes-textarea" placeholder="Write your notes here..."></textarea>
          <div class="notes-modal-footer">
            <button id="notesSaveBtn" class="notes-save-btn">Save</button>
            <button id="notesCancelBtn" class="notes-cancel-btn">Cancel</button>
          </div>
        </div>
      </div>
    `;
    document.body.insertAdjacentHTML('beforeend', modalHTML);
    this.modal = document.getElementById('notesOverlay');
    this.setupEventListeners();
  }

  setupEventListeners() {
    document.getElementById('notesCloseBtn').addEventListener('click', () => this.close());
    document.getElementById('notesCancelBtn').addEventListener('click', () => this.close());
    document.getElementById('notesSaveBtn').addEventListener('click', () => this.saveNote());
  }

  async open(alumniId, alumniName) {
    this.currentAlumniId = alumniId;
    this.currentAlumniName = alumniName;
    document.getElementById('notesTitle').textContent = `Notes for ${alumniName}`;

    // Load existing notes
    await this.loadNote();

    this.modal.style.display = 'flex';
    document.getElementById('notesTextarea').focus();
  }

  close() {
    this.modal.style.display = 'none';
    document.getElementById('notesTextarea').value = '';
  }

  async loadNote() {
    try {
      const response = await fetch(`/api/notes/${this.currentAlumniId}`);
      const data = await response.json();

      if (data.success && data.note) {
        document.getElementById('notesTextarea').value = data.note.note_content;
      } else {
        document.getElementById('notesTextarea').value = '';
      }
    } catch (error) {
      console.error('Error loading note:', error);
      document.getElementById('notesTextarea').value = '';
    }
  }

  async saveNote() {
    const noteContent = document.getElementById('notesTextarea').value;

    try {
      const response = await fetch(`/api/notes/${this.currentAlumniId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ note_content: noteContent })
      });

      const data = await response.json();

      if (data.success) {
        console.log('Note saved successfully');
        // Update cache + rendered button styling for this alumni.
        const hasNote = !!noteContent.trim();
        notesStatusCache[this.currentAlumniId] = hasNote;
        applyNoteIndicatorForId(this.currentAlumniId);
        this.close();
      } else {
        alert('Error saving note: ' + data.error);
      }
    } catch (error) {
      console.error('Error saving note:', error);
      alert('Error saving note');
    }
  }
}

const notesModal = new NotesModal();

// ===== NOTES CACHING & OPTIMIZATION =====
const notesStatusCache = {}; // { id: boolean }
let notesVisibilityObserver = null;
const pendingVisibleNoteIds = new Set();
let visibleNotesFlushTimer = null;
const visibleNotesBatchDelayMs = 140;

async function loadNotesSummary(alumniIds) {
  const ids = Array.from(new Set((alumniIds || []).map(id => parseInt(id, 10)).filter(Number.isFinite)));
  if (!ids.length) return;

  const uncachedIds = ids.filter(id => notesStatusCache[id] === undefined);
  if (!uncachedIds.length) return;

  try {
    // Batch endpoint avoids one /api/notes/<id> call per card on initial render.
    const params = new URLSearchParams();
    params.set('ids', uncachedIds.join(','));
    const response = await fetch(`/api/notes/summary?${params.toString()}`);
    const data = await response.json();
    const summary = (data && data.success && data.summary) ? data.summary : {};

    uncachedIds.forEach(id => {
      notesStatusCache[id] = Boolean(summary[String(id)]);
    });
  } catch (error) {
    console.error('Error loading notes summary:', error);
    uncachedIds.forEach(id => {
      if (notesStatusCache[id] === undefined) notesStatusCache[id] = false;
    });
  }
}

function applyNoteIndicatorForId(alumniId) {
  const hasNote = Boolean(notesStatusCache[alumniId]);
  document.querySelectorAll(`.btn.notes[data-alumni-id="${alumniId}"]`).forEach(btn => {
    if (hasNote) {
      btn.classList.add('has-note');
    } else {
      btn.classList.remove('has-note');
    }
  });
}

async function flushVisibleNotesQueue() {
  visibleNotesFlushTimer = null;
  const ids = Array.from(pendingVisibleNoteIds);
  pendingVisibleNoteIds.clear();
  if (!ids.length) return;

  await loadNotesSummary(ids);
  ids.forEach(applyNoteIndicatorForId);
}

function queueVisibleNotesLoad(alumniId) {
  if (!Number.isFinite(alumniId)) return;
  if (notesStatusCache[alumniId] !== undefined) {
    applyNoteIndicatorForId(alumniId);
    return;
  }

  pendingVisibleNoteIds.add(alumniId);
  if (visibleNotesFlushTimer) return;
  visibleNotesFlushTimer = setTimeout(() => {
    flushVisibleNotesQueue().catch(err => console.error('Error flushing visible notes queue:', err));
  }, visibleNotesBatchDelayMs);
}

function setupVisibleNotesLoading() {
  if (notesVisibilityObserver) {
    notesVisibilityObserver.disconnect();
  }
  pendingVisibleNoteIds.clear();
  if (visibleNotesFlushTimer) {
    clearTimeout(visibleNotesFlushTimer);
    visibleNotesFlushTimer = null;
  }

  const noteButtons = Array.from(document.querySelectorAll('.btn.notes[data-alumni-id]'));
  if (!noteButtons.length) return;

  // Fallback for browsers without IntersectionObserver.
  if (typeof IntersectionObserver === 'undefined') {
    const fallbackIds = noteButtons
      .slice(0, 60)
      .map(btn => parseInt(btn.dataset.alumniId, 10))
      .filter(Number.isFinite);
    loadNotesSummary(fallbackIds)
      .then(() => fallbackIds.forEach(applyNoteIndicatorForId))
      .catch(err => console.error('Error loading fallback notes summary:', err));
    return;
  }

  notesVisibilityObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (!entry.isIntersecting) return;
      const alumniId = parseInt(entry.target.dataset.alumniId, 10);
      queueVisibleNotesLoad(alumniId);
      notesVisibilityObserver.unobserve(entry.target);
    });
  }, {
    root: null,
    rootMargin: '220px 0px',
    threshold: 0.01,
  });

  noteButtons.forEach(btn => {
    const alumniId = parseInt(btn.dataset.alumniId, 10);
    if (!Number.isFinite(alumniId)) return;
    if (notesStatusCache[alumniId] !== undefined) {
      applyNoteIndicatorForId(alumniId);
      return;
    }
    notesVisibilityObserver.observe(btn);
  });
}

async function loadUserInteractions(alumniIds = [], reset = false, requestToken = null) {
  try {
    const ids = Array.from(new Set((alumniIds || []).map(id => parseInt(id, 10)).filter(Number.isFinite)));
    const params = new URLSearchParams();
    if (ids.length) {
      params.set('alumni_ids', ids.join(','));
    }
    const query = params.toString();
    const response = await fetch(`/api/user-interactions${query ? `?${query}` : ''}`);
    const data = await response.json();

    if (requestToken !== null && requestToken !== activeRequestToken) {
      return;
    }

    if (data.success) {
      if (reset) {
        userInteractions = {};
      }
      if (Number.isFinite(data.bookmarked_total)) {
        bookmarkedTotalCount = data.bookmarked_total;
      }

      (data.interactions || []).forEach(interaction => {
        const key = `${interaction.alumni_id}-${interaction.interaction_type}`;
        userInteractions[key] = interaction;
      });
      updateBookmarkCount();
    }
  } catch (error) {
    console.error('Error loading user interactions:', error);
  }
}

async function saveInteraction(alumniId, interactionType, notes = '') {
  try {
    const response = await fetch('/api/interaction', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        alumni_id: alumniId,
        interaction_type: interactionType,
        notes: notes
      })
    });

    const data = await response.json();

    if (data.success) {
      console.log(`${interactionType} saved for alumni ${alumniId}`);
      // Update local state
      const key = `${alumniId}-${interactionType}`;
      userInteractions[key] = { alumni_id: alumniId, interaction_type: interactionType, notes };
      if (interactionType === 'bookmarked') {
        bookmarkedTotalCount = (Number.isFinite(bookmarkedTotalCount) ? bookmarkedTotalCount : 0) + 1;
      }
      return true;
    } else {
      console.error('Error saving interaction:', data.error);
      return false;
    }
  } catch (error) {
    console.error('Error saving interaction:', error);
    return false;
  }
}

async function removeInteraction(alumniId, interactionType) {
  try {
    const response = await fetch('/api/interaction', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        alumni_id: alumniId,
        interaction_type: interactionType
      })
    });

    const data = await response.json();

    if (data.success) {
      console.log(`${interactionType} removed for alumni ${alumniId}`);
      // Update local state
      const key = `${alumniId}-${interactionType}`;
      delete userInteractions[key];
      if (interactionType === 'bookmarked') {
        bookmarkedTotalCount = Math.max(0, (Number.isFinite(bookmarkedTotalCount) ? bookmarkedTotalCount : 0) - 1);
      }
      return true;
    } else {
      console.error('Error removing interaction:', data.error);
      return false;
    }
  } catch (error) {
    console.error('Error removing interaction:', error);
    return false;
  }
}

// Check if user has an interaction for an alumni
function hasInteraction(alumniId, interactionType) {
  const key = `${alumniId}-${interactionType}`;
  return key in userInteractions;
}

function _isMeaningfulEducationValue(value) {
  if (!value) return false;
  const trimmed = String(value).trim();
  if (!trimmed) return false;

  const lowered = trimmed.toLowerCase();
  if (['unknown', 'not found', 'n/a', 'na', 'none', 'null', 'nan', 'other'].includes(lowered)) {
    return false;
  }
  if (lowered === 'university of north texas' || lowered === 'unt') {
    return false;
  }
  return true;
}

function _isMeaningfulLocationValue(value) {
  if (!value) return false;
  const trimmed = String(value).trim();
  if (!trimmed) return false;
  const lowered = trimmed.toLowerCase();
  return !['unknown', 'not found', 'n/a', 'na', 'none', 'null', 'nan'].includes(lowered);
}

function _buildEducationLine(profile) {
  const fullDegree = _isMeaningfulEducationValue(profile.full_degree) ? String(profile.full_degree).trim() : '';
  const fullMajor = _isMeaningfulEducationValue(profile.full_major) ? String(profile.full_major).trim() : '';

  if (!fullDegree && !fullMajor) return '';
  if (fullDegree && fullMajor) {
    if (fullDegree.toLowerCase().includes(fullMajor.toLowerCase())) {
      return fullDegree;
    }
    return `${fullDegree} - ${fullMajor}`;
  }
  return fullDegree || fullMajor;
}

function _buildClassLocationLine(profile) {
  const gradYearRaw = profile.class ? String(profile.class).trim() : '';
  const gradYear = gradYearRaw.replace(/\b(19\d{2}|20\d{2}|2100)\.0\b/g, '$1');
  const location = _isMeaningfulLocationValue(profile.location) ? String(profile.location).trim() : '';

  if (gradYear) {
    return `Class of ${gradYear}${location ? ' - ' + location : ''}`;
  }
  return location;
}

// Create profile list item element (horizontal row)
function createListItem(p) {
  const educationLine = _buildEducationLine(p);
  const roleLine = p.role || p.headline || '';
  const classLocationLine = _buildClassLocationLine(p);

  const item = document.createElement('div');
  item.className = 'list-item';
  item.setAttribute('data-id', p.id);
  item.innerHTML = `
      <div class="list-main">
        <div class="list-details">
          <h3 class="name">${p.name}</h3>
          ${educationLine ? `<p class="education">${educationLine}</p>` : ''}
          ${roleLine ? `<p class="role">${roleLine}</p>` : ''}
          ${classLocationLine ? `<div class="class">${classLocationLine}</div>` : ''}
        </div>
        <div class="list-actions">

          <button class="btn profile-view" type="button" title="View full profile" data-alumni-id="${p.id}">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18">
              <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
              <circle cx="12" cy="12" r="3"></circle>
            </svg>
          </button>

          <a class="btn link" href="${p.linkedin}" target="_blank" rel="noopener" title="View LinkedIn Profile">
            <img src="/assets/linkedin.svg" alt="LinkedIn" class="linkedin-icon" />
          </a>
          <button class="btn connect" type="button">Connect</button>
          <button class="btn star" type="button" title="Bookmark this alumni">&#9734;</button>
          <button class="btn notes" type="button" title="Add note" data-alumni-id="${p.id}" data-alumni-name="${p.name}"><img src="/assets/note.svg" alt="Notes" class="notes-icon" /></button>
        </div>
      </div>
    `;



  // Connect button action - TOGGLE between Connect and Requested
  const connectBtn = item.querySelector('.btn.connect');
  if (hasInteraction(p.id, 'connected')) {
    connectBtn.textContent = 'Requested';
    connectBtn.classList.add('requested');
  }
  connectBtn.addEventListener('click', async () => {
    const isCurrentlyConnected = connectBtn.classList.contains('requested');
    if (isCurrentlyConnected) {
      const success = await removeInteraction(p.id, 'connected');
      if (success) {
        connectBtn.textContent = 'Connect';
        connectBtn.classList.remove('requested');
      }
    } else {
      const success = await saveInteraction(p.id, 'connected');
      if (success) {
        connectBtn.textContent = 'Requested';
        connectBtn.classList.add('requested');
      }
    }
  });

  // Bookmark button action - toggle between hollow and filled star.
  const starBtn = item.querySelector('.btn.star');
  if (hasInteraction(p.id, 'bookmarked')) {
    item.classList.add('bookmarked');
    starBtn.innerHTML = '&#9733;';
  }
  starBtn.addEventListener('click', async () => {
    const isCurrentlyBookmarked = item.classList.contains('bookmarked');
    if (isCurrentlyBookmarked) {
      const success = await removeInteraction(p.id, 'bookmarked');
      if (success) {
        item.classList.remove('bookmarked');
        starBtn.innerHTML = '&#9734;';
        updateBookmarkCount(); // Update banner count
      }
    } else {
      const success = await saveInteraction(p.id, 'bookmarked');
      if (success) {
        item.classList.add('bookmarked');
        starBtn.innerHTML = '&#9733;';
        updateBookmarkCount(); // Update banner count
      }
    }
  });

  // Profile view button action - OPEN PROFILE DETAIL MODAL
  const profileViewBtn = item.querySelector('.btn.profile-view');
  if (profileViewBtn) {
    profileViewBtn.addEventListener('click', () => {
      if (typeof profileDetailModal !== 'undefined') {
        profileDetailModal.open(p, profileViewBtn);
      }
    });
  }

  // Notes button action - OPEN MODAL
  const notesBtn = item.querySelector('.btn.notes');
  if (notesStatusCache[p.id]) {
    notesBtn.classList.add('has-note');
  }
  notesBtn.addEventListener('click', async () => {
    const alumniId = parseInt(notesBtn.dataset.alumniId);
    const alumniName = notesBtn.dataset.alumniName;
    notesModal.open(alumniId, alumniName);
  });

  return item;
}

function renderProfiles(list) {
  const safeList = Array.isArray(list) ? list : [];

  if (listContainer) {
    listContainer.innerHTML = '';
    safeList.forEach(p => listContainer.appendChild(createListItem(p)));
  }

  if (count) {
    count.textContent = Number.isFinite(totalAlumniCount) && totalAlumniCount > safeList.length
      ? `(${safeList.length} of ${totalAlumniCount})`
      : `(${safeList.length})`;
  }

  updateStatsBanner(safeList);
  setupVisibleNotesLoading();
  renderLoadMoreControl();
}

function renderLoadMoreControl() {
  const paginationContainer = document.getElementById('pagination');
  if (!paginationContainer) return;

  paginationContainer.innerHTML = '';
  paginationContainer.style.display = 'flex';

  const status = document.createElement('span');
  status.className = 'pagination-ellipsis';
  if (isLoadingAlumni) {
    status.textContent = Number.isFinite(totalAlumniCount) && totalAlumniCount > 0
      ? `Loading alumni... ${loadedAlumni.length}/${totalAlumniCount}`
      : `Loading alumni... ${loadedAlumni.length}`;
  } else {
    status.textContent = Number.isFinite(totalAlumniCount)
      ? `${loadedAlumni.length}/${totalAlumniCount} loaded`
      : `${loadedAlumni.length} loaded`;
  }
  paginationContainer.appendChild(status);
}

function getCanonicalRoleTitle(value) {
  const title = (value || '').trim();
  if (!title) return '';
  const low = title.toLowerCase().replace(/\s+/g, ' ');

  if (low === 'director' || low === 'director of' || low === 'director of engineering') {
    return 'Director';
  }
  if (
    low === 'manager'
    || low === 'manager - innovation'
    || low === 'laboratory safety manager'
    || low === 'senior manager - innovation'
  ) {
    return 'Manager';
  }
  return title;
}

function populateFilters(list) {
  const isValid = val => val && !['Unknown', 'Not Found', 'N/A'].includes(val);

  function getNormalizedCompany(name) {
    if (!name) return "";
    if (name.includes("Dallas")) return name;
    if (name.includes("University of North Texas") || name.startsWith("UNT ") || name === "UNT" || name.includes(" UNT ") || name.endsWith(" UNT")) {
      return "University of North Texas";
    }
    return name;
  }

  const locations = Array.from(new Set(list.map(x => x.location).filter(isValid))).sort();
  const roles = Array.from(
    new Set(
      list
        .map(x => getCanonicalRoleTitle(x.normalized_title || x.role || x.current_job_title))
        .filter(isValid)
    )
  ).sort();
  const companies = Array.from(new Set(list.map(x => getNormalizedCompany(x.company)).filter(isValid))).sort();
  const majors = Array.from(new Set(list.map(x => x.major).filter(Boolean)))
    .filter(m => APPROVED_ENGINEERING_DISCIPLINES.includes(m))
    .sort();
  const years = Array.from(new Set(list.map(x => x.class).filter(Boolean))).sort((a, b) => b - a);
  const degrees = ['Undergraduate', 'Graduate', 'PhD'].filter(level =>
    list.some(x => x.degree === level && isValid(level))
  );

  const selectedLocations = new Set(Array.from(document.querySelectorAll('input[name="location"]:checked')).map(i => i.value));
  const selectedRoles = new Set(Array.from(document.querySelectorAll('input[name="role"]:checked')).map(i => i.value));
  const selectedCompanies = new Set(Array.from(document.querySelectorAll('input[name="company"]:checked')).map(i => i.value));
  const selectedMajors = new Set(Array.from(document.querySelectorAll('input[name="major"]:checked')).map(i => i.value));
  const selectedDegrees = new Set(Array.from(document.querySelectorAll('input[name="degree"]:checked')).map(i => i.value));
  const currentYearValue = (document.getElementById('gradSelect') || {}).value || '';

  const locChecks = document.getElementById('locChecks');
  const roleChecks = document.getElementById('roleChecks');
  const companyChecks = document.getElementById('companyChecks');
  const majorChecks = document.getElementById('majorChecks');
  const gradSelect = document.getElementById('gradSelect');
  const degreeChecks = document.getElementById('degreeChecks');

  if (locChecks) {
    locChecks.innerHTML = '';
    locations.forEach(v => {
      const label = document.createElement('label');
      label.className = 'check';
      label.innerHTML = `<input type="checkbox" name="location" value="${v}" ${selectedLocations.has(v) ? 'checked' : ''} /> <span>${v}</span>`;
      locChecks.appendChild(label);
    });
  }

  if (roleChecks) {
    roleChecks.innerHTML = '';
    roles.forEach(v => {
      const label = document.createElement('label');
      label.className = 'check';
      label.innerHTML = `<input type="checkbox" name="role" value="${v}" ${selectedRoles.has(v) ? 'checked' : ''} /> <span>${v}</span>`;
      roleChecks.appendChild(label);
    });
  }

  if (companyChecks) {
    companyChecks.innerHTML = '';
    companies.forEach(v => {
      const label = document.createElement('label');
      label.className = 'check';
      label.innerHTML = `<input type="checkbox" name="company" value="${v}" ${selectedCompanies.has(v) ? 'checked' : ''} /> <span>${v}</span>`;
      companyChecks.appendChild(label);
    });
  }

  if (majorChecks) {
    majorChecks.innerHTML = '';
    majors.forEach(v => {
      const label = document.createElement('label');
      label.className = 'check';
      label.innerHTML = `<input type="checkbox" name="major" value="${v}" ${selectedMajors.has(v) ? 'checked' : ''} /> <span>${v}</span>`;
      majorChecks.appendChild(label);
    });
  }

  if (gradSelect) {
    gradSelect.innerHTML = '<option value="">All years</option>';
    years.forEach(y => {
      const opt = document.createElement('option');
      opt.value = y;
      opt.textContent = y;
      gradSelect.appendChild(opt);
    });
    gradSelect.value = currentYearValue;
  }

  if (degreeChecks) {
    degreeChecks.innerHTML = '';
    degrees.forEach(v => {
      const label = document.createElement('label');
      label.className = 'check';
      label.innerHTML = `<input type="checkbox" name="degree" value="${v}" ${selectedDegrees.has(v) ? 'checked' : ''} /> <span>${v}</span>`;
      degreeChecks.appendChild(label);
    });
  }
}

function mapAlumniRecord(a) {
  return {
    id: a.id,
    name: a.name || '',
    role: a.role || '',
    normalized_title: a.normalized_title || '',
    company: a.company || '',
    class: a.class || '',
    location: a.location || '',
    headline: a.headline || '',
    linkedin: a.linkedin || '',
    degree: a.degree || '',
    full_degree: a.full_degree || '',
    full_major: a.full_major || '',
    major: a.major || '',
    updated_at: a.updated_at || '',
    working_while_studying: a.working_while_studying !== undefined ? a.working_while_studying : null,
    unt_alumni_status: a.unt_alumni_status || 'unknown'
  };
}

function collectQueryState() {
  const q = document.getElementById('q');
  const gradSelect = document.getElementById('gradSelect');
  const sortSelect = document.getElementById('sortSelect');

  const term = q ? q.value.trim() : '';
  const loc = Array.from(document.querySelectorAll('input[name="location"]:checked')).map(i => i.value);
  const role = Array.from(document.querySelectorAll('input[name="role"]:checked')).map(i => i.value);
  const company = Array.from(document.querySelectorAll('input[name="company"]:checked')).map(i => i.value);
  const major = Array.from(document.querySelectorAll('input[name="major"]:checked')).map(i => i.value);
  const degree = Array.from(document.querySelectorAll('input[name="degree"]:checked')).map(i => i.value);
  const year = gradSelect ? gradSelect.value : '';
  const wwsRadio = document.querySelector('input[name="workingWhileStudying"]:checked');
  const wws = wwsRadio ? wwsRadio.value : '';
  const untAlumniStatusRadio = document.querySelector('input[name="untAlumniStatus"]:checked');
  const untAlumniStatus = untAlumniStatusRadio ? untAlumniStatusRadio.value : '';

  const sortValue = sortSelect ? (sortSelect.value || '') : '';
  const bookmarkedOnly = sortValue === 'bookmarked';
  const sort = bookmarkedOnly ? 'name' : (sortValue || 'name');

  return {
    term,
    loc,
    role,
    company,
    major,
    degree,
    year,
    wws,
    untAlumniStatus,
    sort,
    bookmarkedOnly,
    direction: sortDirection,
  };
}

function buildAlumniQueryParams(queryState, offset, limit) {
  const params = new URLSearchParams();
  params.set('limit', String(limit));
  params.set('offset', String(offset));

  if (queryState.term) params.set('q', queryState.term);
  if (queryState.loc.length) params.set('location', queryState.loc.join(','));
  if (queryState.role.length) params.set('role', queryState.role.join(','));
  if (queryState.company.length) params.set('company', queryState.company.join(','));
  if (queryState.major.length) params.set('major', queryState.major.join(','));
  if (queryState.degree.length) params.set('degree', queryState.degree.join(','));
  if (queryState.year) params.set('grad_year', queryState.year);
  if (queryState.wws) params.set('working_while_studying', queryState.wws);
  if (queryState.untAlumniStatus) params.set('unt_alumni_status', queryState.untAlumniStatus);
  if (queryState.sort) params.set('sort', queryState.sort);
  params.set('direction', queryState.direction);
  if (queryState.bookmarkedOnly) params.set('bookmarked_only', '1');

  return params;
}

async function fetchAlumniPage({ reset = false, initializeFilters = false } = {}) {
  const requestToken = ++activeRequestToken;

  if (reset) {
    loadedAlumni = [];
    totalAlumniCount = 0;
  }

  const queryState = collectQueryState();
  activeQueryState = queryState;

  isLoadingAlumni = true;
  renderProfiles(loadedAlumni);
  renderLoadMoreControl();

  try {
    if (reset) {
      await loadUserInteractions([], true, requestToken);
    }

    let offset = 0;
    let hasMore = true;
    const seenIds = new Set();

    // Auto-page through all results so the full dataset is loaded without manual "Load more".
    while (hasMore) {
      if (requestToken !== activeRequestToken) {
        return;
      }

      const params = buildAlumniQueryParams(queryState, offset, alumniChunkSize);
      const resp = await fetch(`/api/alumni?${params.toString()}`);
      const data = await resp.json();
      const items = Array.isArray(data.items)
        ? data.items
        : (Array.isArray(data.alumni) ? data.alumni : []);

      if (Number.isFinite(data.total)) {
        totalAlumniCount = data.total;
      }

      const mapped = items.map(mapAlumniRecord);
      mapped.forEach(item => {
        if (!seenIds.has(item.id)) {
          seenIds.add(item.id);
          loadedAlumni.push(item);
        }
      });

      offset = loadedAlumni.length;
      hasMore = Boolean(data.has_more) && items.length > 0;

      if (requestToken !== activeRequestToken) {
        return;
      }
      renderProfiles(loadedAlumni);
    }

    if (!Number.isFinite(totalAlumniCount) || totalAlumniCount <= 0) {
      totalAlumniCount = loadedAlumni.length;
    }

    if (initializeFilters && !filtersInitialized && loadedAlumni.length) {
      populateFilters(loadedAlumni);
      filtersInitialized = true;
    }

    renderProfiles(loadedAlumni);
  } catch (err) {
    console.error('Error fetching alumni from API', err);
    if (reset && requestToken === activeRequestToken) {
      loadedAlumni = fakeAlumni.map(mapAlumniRecord);
      totalAlumniCount = loadedAlumni.length;
      if (initializeFilters && !filtersInitialized) {
        populateFilters(loadedAlumni);
        filtersInitialized = true;
      }
      renderProfiles(loadedAlumni);
    }
  } finally {
    if (requestToken === activeRequestToken) {
      isLoadingAlumni = false;
      renderLoadMoreControl();
    }
  }
}

function updateSortLabel() {
  const sortSelect = document.getElementById('sortSelect');
  const sortLabel = document.getElementById('sortLabel');
  if (!sortSelect || !sortLabel) return;

  const val = sortSelect.value;
  let text = "";
  if (val === 'name') {
    text = sortDirection === 'asc' ? "(A -> Z)" : "(Z -> A)";
  } else if (val === 'year') {
    text = sortDirection === 'asc' ? "(Oldest -> Newest)" : "(Newest -> Oldest)";
  } else if (val === 'updated') {
    text = sortDirection === 'asc' ? "(Oldest -> Newest)" : "(Newest -> Oldest)";
  }
  sortLabel.textContent = text;
}

function setupFiltering() {
  const q = document.getElementById('q');
  const gradSelect = document.getElementById('gradSelect');
  const sortSelect = document.getElementById('sortSelect');
  const sortReverseBtn = document.getElementById('sortReverseBtn');
  const clearBtn = document.getElementById('clear-filters');

  let searchDebounce = null;
  const applyFilters = async () => {
    await fetchAlumniPage({ reset: true });
  };

  if (clearBtn) {
    clearBtn.addEventListener('click', async () => {
      document.querySelectorAll('input[name="location"], input[name="role"], input[name="company"], input[name="major"], input[name="degree"]').forEach(cb => cb.checked = false);
      if (gradSelect) gradSelect.value = '';
      if (q) q.value = '';
      const wwsAll = document.querySelector('input[name="workingWhileStudying"][value=""]');
      if (wwsAll) wwsAll.checked = true;
      const untStatusAll = document.querySelector('input[name="untAlumniStatus"][value=""]');
      if (untStatusAll) untStatusAll.checked = true;
      await applyFilters();
    });
  }

  if (q) {
    q.addEventListener('input', () => {
      clearTimeout(searchDebounce);
      searchDebounce = setTimeout(() => {
        applyFilters();
      }, 250);
    });
  }

  document.addEventListener('change', (e) => {
    if (e.target.matches('input[name="location"], input[name="role"], input[name="company"], input[name="major"], input[name="degree"], #gradSelect, input[name="workingWhileStudying"], input[name="untAlumniStatus"]')) {
      applyFilters();
    }
  });

  if (sortSelect) {
    sortSelect.addEventListener('change', () => {
      updateSortLabel();
      applyFilters();
    });
  }

  if (sortReverseBtn) {
    sortReverseBtn.addEventListener('click', () => {
      sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
      updateSortLabel();
      applyFilters();
    });
  }

  window.applyFiltersAndSort = applyFilters;
  updateSortLabel();
}

// Initialize and auto-load all alumni rows for the active query in backend-sized chunks.
(async function init() {
  notesModal.create();
  setupFiltering();
  await fetchAlumniPage({ reset: true, initializeFilters: true });
})();
