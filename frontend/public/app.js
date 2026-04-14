// app.js
// Approved engineering disciplines (must match backend APPROVED_ENGINEERING_DISCIPLINES)
const APPROVED_ENGINEERING_DISCIPLINES = [
  'Software, Data, AI & Cybersecurity',
  'Embedded, Electrical & Hardware Engineering',
  'Mechanical Engineering & Manufacturing',
  'Biomedical Engineering',
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
let currentDirectoryPage = 1;
// Backend currently caps alumni page size at 500.
const alumniChunkSize = 500;
const directoryPageSize = 25;

const ALUMNI_EXPORT_FIELDS = Object.freeze([
  'first',
  'last',
  'linkedin_url',
  'school',
  'degree',
  'major',
  'school_start',
  'grad_year',
  'school2',
  'degree2',
  'major2',
  'school3',
  'degree3',
  'major3',
  'discipline',
  'location',
  'working_while_studying',
  'title',
  'company',
  'job_employment_type',
  'job_start',
  'job_end',
  'exp_2_title',
  'exp_2_company',
  'exp_2_dates',
  'exp_2_employment_type',
  'exp_3_title',
  'exp_3_company',
  'exp_3_dates',
  'exp_3_employment_type',
  'seniority_level',
]);

const listContainer = document.getElementById('list');
const count = document.getElementById('count');

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function isWorkingWhileStudyingPositive(value) {
  if (value === true || value === 1) return true;
  if (typeof value !== 'string') return false;
  const normalized = value.trim().toLowerCase();
  return normalized === 'yes' || normalized === 'currently' || normalized === 'true' || normalized === '1';
}

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
  const wwsCount = safeAlumni.filter(a => isWorkingWhileStudyingPositive(a.working_while_studying)).length;

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

/**
 * Modal for editing alumni entries.
 * Provides form validation on both client and server side.
 */
class EditModal {
  constructor() {
    this.currentAlumniId = null;
    this.currentAlumniData = null;
    this.setupModal();
  }

  setupModal() {
    // Create modal HTML if it doesn't exist
    if (!document.getElementById('editModal')) {
      const modalHTML = `
        <div id="editModal" class="modal">
          <div class="modal-content" style="max-width: 600px; max-height: 80vh; overflow-y: auto;">
            <div class="modal-header">
              <h2>Edit Alumni Entry</h2>
              <button type="button" class="modal-close" id="editModalClose">&times;</button>
            </div>
            <div class="modal-body">
              <form id="editForm">
                <div class="form-group">
                  <label for="editFirstName">First Name</label>
                  <input type="text" id="editFirstName" name="first_name" maxlength="100">
                  <small class="error" id="editFirstNameError"></small>
                </div>

                <div class="form-group">
                  <label for="editLastName">Last Name</label>
                  <input type="text" id="editLastName" name="last_name" maxlength="100">
                  <small class="error" id="editLastNameError"></small>
                </div>

                <div class="form-group">
                  <label for="editGradYear">Graduation Year</label>
                  <input type="number" id="editGradYear" name="grad_year" min="1900" max="2100">
                  <small class="error" id="editGradYearError"></small>
                </div>

                <div class="form-group">
                  <label for="editDegree">Degree</label>
                  <input type="text" id="editDegree" name="degree" maxlength="255">
                  <small class="error" id="editDegreeError"></small>
                </div>

                <div class="form-group">
                  <label for="editMajor">Major</label>
                  <input type="text" id="editMajor" name="major" maxlength="255">
                  <small class="error" id="editMajorError"></small>
                </div>

                <div class="form-group">
                  <label for="editLocation">Location</label>
                  <input type="text" id="editLocation" name="location" maxlength="255">
                  <small class="error" id="editLocationError"></small>
                </div>

                <div class="form-group">
                  <label for="editHeadline">Headline</label>
                  <input type="text" id="editHeadline" name="headline" maxlength="500">
                  <small class="error" id="editHeadlineError"></small>
                </div>

                <div class="form-group">
                  <label for="editJobTitle">Job Title</label>
                  <input type="text" id="editJobTitle" name="current_job_title" maxlength="255">
                  <small class="error" id="editJobTitleError"></small>
                </div>

                <div class="form-group">
                  <label for="editCompany">Company</label>
                  <input type="text" id="editCompany" name="company" maxlength="255">
                  <small class="error" id="editCompanyError"></small>
                </div>

                <div class="form-group">
                  <label for="editJobStartDate">Job Start Date</label>
                  <input type="text" id="editJobStartDate" name="job_start_date" placeholder="e.g., Jan 2020">
                  <small class="error" id="editJobStartDateError"></small>
                </div>

                <div class="form-group">
                  <label for="editJobEndDate">Job End Date</label>
                  <input type="text" id="editJobEndDate" name="job_end_date" placeholder="e.g., Dec 2023">
                  <small class="error" id="editJobEndDateError"></small>
                </div>

                <div class="form-group">
                  <label for="editWWS">Working While Studying</label>
                  <select id="editWWS" name="working_while_studying_status">
                    <option value="">Not specified</option>
                    <option value="yes">Yes</option>
                    <option value="no">No</option>
                    <option value="currently">Currently (or other)</option>
                  </select>
                  <small class="error" id="editWWSError"></small>
                </div>

                <div class="form-group" style="margin-top: 10px;">
                  <label style="display:flex; align-items:center; gap:8px; font-weight:600;">
                    <input type="checkbox" id="editStandardizeWithGroq" name="standardize_with_groq" checked>
                    Standardize with Groq (if available)
                  </label>
                  <small class="hint" style="display:block; color:#666; margin-top:4px;">
                    If unchecked, raw and normalized values are stored exactly as entered.
                  </small>
                </div>
              </form>
            </div>
            <div class="modal-footer">
              <button type="button" class="btn secondary" id="editModalCancel">Cancel</button>
              <button type="button" class="btn primary" id="editModalSave">Save Changes</button>
            </div>
          </div>
        </div>
      `;
      document.body.insertAdjacentHTML('beforeend', modalHTML);
    }

    // Setup event listeners
    document.getElementById('editModalClose').addEventListener('click', () => this.close());
    document.getElementById('editModalCancel').addEventListener('click', () => this.close());
    document.getElementById('editModalSave').addEventListener('click', () => this.saveChanges());
  }

  async open(alumniId, alumniData) {
    this.currentAlumniId = alumniId;
    this.currentAlumniData = alumniData;

    let source = alumniData || {};
    try {
      // Fetch full record so edit form prefill is complete even when list payload is partial.
      const resp = await fetch(`/api/alumni/${alumniId}`);
      const payload = await resp.json();
      if (resp.ok && payload && payload.success && payload.alumni) {
        source = payload.alumni;
      }
    } catch (err) {
      console.warn('Falling back to list payload for edit prefill:', err);
    }

    // Populate form with current data
    document.getElementById('editFirstName').value = (source.first_name || source.first || '').trim();
    document.getElementById('editLastName').value = (source.last_name || source.last || '').trim();
    document.getElementById('editGradYear').value = source.grad_year || source.class || '';
    document.getElementById('editDegree').value = (source.degree_raw || source.full_degree || source.degree || '').trim();
    document.getElementById('editMajor').value = (source.major_raw || source.full_major || source.major || '').trim();
    document.getElementById('editLocation').value = (source.location || '').trim();
    document.getElementById('editHeadline').value = (source.headline || '').trim();
    document.getElementById('editJobTitle').value = (source.current_job_title || source.title || source.role || '').trim();
    document.getElementById('editCompany').value = (source.company || '').trim();
    document.getElementById('editJobStartDate').value = (source.job_start_date || source.job_start || '').trim();
    document.getElementById('editJobEndDate').value = (source.job_end_date || source.job_end || '').trim();
    document.getElementById('editWWS').value = (source.working_while_studying_status || source.working_while_studying || '').toString().trim();
    document.getElementById('editStandardizeWithGroq').checked = true;

    // Clear all error messages
    document.querySelectorAll('#editModal .error').forEach(el => el.textContent = '');

    // Show modal with flex display
    const modal = document.getElementById('editModal');
    modal.style.display = 'flex';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    document.body.style.overflow = 'hidden';
  }

  close() {
    document.getElementById('editModal').style.display = 'none';
    document.body.style.overflow = 'auto';
    this.currentAlumniId = null;
    this.currentAlumniData = null;
  }

  async saveChanges() {
    // Clear previous errors
    document.querySelectorAll('.error').forEach(el => el.textContent = '');

    const formData = {
      first_name: document.getElementById('editFirstName').value.trim(),
      last_name: document.getElementById('editLastName').value.trim(),
      grad_year: document.getElementById('editGradYear').value ? parseInt(document.getElementById('editGradYear').value) : null,
      degree: document.getElementById('editDegree').value.trim(),
      major: document.getElementById('editMajor').value.trim(),
      location: document.getElementById('editLocation').value.trim(),
      headline: document.getElementById('editHeadline').value.trim(),
      current_job_title: document.getElementById('editJobTitle').value.trim(),
      company: document.getElementById('editCompany').value.trim(),
      job_start_date: document.getElementById('editJobStartDate').value.trim(),
      job_end_date: document.getElementById('editJobEndDate').value.trim(),
      working_while_studying_status: document.getElementById('editWWS').value.trim(),
      standardize_with_groq: !!document.getElementById('editStandardizeWithGroq').checked,
    };

    // Remove empty values
    Object.keys(formData).forEach(key => {
      if (!formData[key]) delete formData[key];
    });

    try {
      const response = await fetch(`/api/alumni/${this.currentAlumniId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData)
      });

      const data = await response.json();

      if (data.success) {
        alert('Changes saved successfully!');
        this.close();
        // Reload the alumni list to reflect changes
        if (activeQueryState) {
          queryAlumni(activeQueryState);
        }
      } else if (data.errors) {
        // Display server-side validation errors
        Object.keys(data.errors).forEach(fieldName => {
          const errorEl = document.getElementById(`edit${fieldName.charAt(0).toUpperCase() + fieldName.slice(1).replace(/_/g, '')}Error`);
          if (errorEl) {
            errorEl.textContent = data.errors[fieldName];
          }
        });
        alert('Validation errors - please check the form');
      } else {
        alert('Error saving changes: ' + (data.error || 'Unknown error'));
      }
    } catch (error) {
      console.error('Error saving changes:', error);
      alert('Error saving changes: ' + error.message);
    }
  }
}

const editModal = new EditModal();

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

function _sanitizeEducationSnippet(value) {
  if (!value) return '';
  let text = String(value).trim();
  if (!text) return '';

  // Remove prefixed date ranges mistakenly captured into degree/major fields.
  text = text.replace(
    /^\s*(?:[A-Za-z]{3,9}\s+)?(?:19|20)\d{2}\s*[-–—]\s*(?:[A-Za-z]{3,9}\s+)?(?:(?:19|20)\d{2}|Present)\s*[-–—:]\s*/i,
    ''
  );

  // Remove stale "Class of YYYY" prefixes if they leaked into education text.
  text = text.replace(/^\s*Class\s+of\s+(?:19|20)\d{2}\s*[-–—:]\s*/i, '');

  // If only a date span remains, suppress it from the education line.
  if (/^(?:[A-Za-z]{3,9}\s+)?(?:19|20)\d{2}\s*[-–—]\s*(?:[A-Za-z]{3,9}\s+)?(?:(?:19|20)\d{2}|Present)$/i.test(text)) {
    return '';
  }

  return text.replace(/\s{2,}/g, ' ').trim();
}

function _isMeaningfulLocationValue(value) {
  if (!value) return false;
  const trimmed = String(value).trim();
  if (!trimmed) return false;
  const lowered = trimmed.toLowerCase();
  return !['unknown', 'not found', 'n/a', 'na', 'none', 'null', 'nan'].includes(lowered);
}

function _buildEducationLine(profile) {
  const fullDegree = _isMeaningfulEducationValue(profile.full_degree)
    ? _sanitizeEducationSnippet(profile.full_degree)
    : '';
  const fullMajor = _isMeaningfulEducationValue(profile.full_major)
    ? _sanitizeEducationSnippet(profile.full_major)
    : '';

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
          <button class="btn notes" type="button" title="Add note" data-alumni-id="${p.id}" data-alumni-name="${p.name}">
            <svg viewBox="0 0 24 24" fill="currentColor" width="20" height="20">
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
            </svg>
          </button>
          <button class="btn edit" type="button" title="Edit entry" data-alumni-id="${p.id}">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18">
              <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
              <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
            </svg>
          </button>
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

  // Edit button action - OPEN EDIT MODAL
  const editBtn = item.querySelector('.btn.edit');
  if (editBtn) {
    editBtn.addEventListener('click', () => {
      const alumniId = parseInt(editBtn.dataset.alumniId);
      if (typeof editModal !== 'undefined') {
        editModal.open(alumniId, p);
      }
    });
  }

  return item;
}

function renderProfiles(list) {
  const safeList = Array.isArray(list) ? list : [];
  const resolvedTotal = Number.isFinite(totalAlumniCount) && totalAlumniCount > 0
    ? totalAlumniCount
    : safeList.length;
  const totalPages = Math.max(1, Math.ceil(resolvedTotal / directoryPageSize));

  if (currentDirectoryPage > totalPages) {
    currentDirectoryPage = totalPages;
  }
  if (currentDirectoryPage < 1) {
    currentDirectoryPage = 1;
  }

  const startIndex = (currentDirectoryPage - 1) * directoryPageSize;
  const pageSlice = safeList.slice(startIndex, startIndex + directoryPageSize);

  if (listContainer) {
    listContainer.innerHTML = '';
    pageSlice.forEach(p => listContainer.appendChild(createListItem(p)));
  }

  if (count) {
    if (!resolvedTotal || pageSlice.length === 0) {
      count.textContent = '(0)';
    } else {
      const startDisplay = startIndex + 1;
      const endDisplay = startIndex + pageSlice.length;
      count.textContent = `(${startDisplay}-${endDisplay} of ${resolvedTotal})`;
    }
  }

  updateStatsBanner(safeList);
  setupVisibleNotesLoading();
  renderLoadMoreControl();
}

function goToDirectoryPage(page) {
  const resolvedTotal = Number.isFinite(totalAlumniCount) && totalAlumniCount > 0
    ? totalAlumniCount
    : loadedAlumni.length;
  const totalPages = Math.max(1, Math.ceil(resolvedTotal / directoryPageSize));
  const targetPage = Math.max(1, Math.min(totalPages, Number(page) || 1));
  if (targetPage === currentDirectoryPage) return;

  currentDirectoryPage = targetPage;
  renderProfiles(loadedAlumni);
}

function createPaginationButton(label, page, { disabled = false, active = false } = {}) {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = active ? 'pagination-btn active' : 'pagination-btn';
  btn.textContent = label;
  btn.disabled = disabled;
  if (active) {
    btn.setAttribute('aria-current', 'page');
  }
  if (!disabled && !active) {
    btn.addEventListener('click', () => goToDirectoryPage(page));
  }
  return btn;
}

function renderLoadMoreControl() {
  const paginationContainer = document.getElementById('pagination');
  if (!paginationContainer) return;

  paginationContainer.innerHTML = '';
  paginationContainer.style.display = 'flex';
  paginationContainer.style.flexWrap = 'wrap';

  const resolvedTotal = Number.isFinite(totalAlumniCount) && totalAlumniCount > 0
    ? totalAlumniCount
    : loadedAlumni.length;

  if (isLoadingAlumni) {
    const status = document.createElement('span');
    status.className = 'pagination-ellipsis';
    status.textContent = Number.isFinite(totalAlumniCount) && totalAlumniCount > 0
      ? `Loading alumni... ${loadedAlumni.length}/${totalAlumniCount}`
      : `Loading alumni... ${loadedAlumni.length}`;
    paginationContainer.appendChild(status);
    return;
  }

  if (resolvedTotal <= 0) {
    const empty = document.createElement('span');
    empty.className = 'pagination-ellipsis';
    empty.textContent = 'No alumni found';
    paginationContainer.appendChild(empty);
    return;
  }

  const totalPages = Math.max(1, Math.ceil(resolvedTotal / directoryPageSize));
  const startDisplay = (currentDirectoryPage - 1) * directoryPageSize + 1;
  const endDisplay = Math.min(currentDirectoryPage * directoryPageSize, resolvedTotal);

  const status = document.createElement('span');
  status.className = 'pagination-ellipsis';
  status.textContent = `Showing ${startDisplay}-${endDisplay} of ${resolvedTotal}`;
  paginationContainer.appendChild(status);

  paginationContainer.appendChild(
    createPaginationButton('Prev', currentDirectoryPage - 1, { disabled: currentDirectoryPage <= 1 })
  );

  const maxVisiblePages = 5;
  let pageStart = Math.max(1, currentDirectoryPage - Math.floor(maxVisiblePages / 2));
  let pageEnd = Math.min(totalPages, pageStart + maxVisiblePages - 1);

  if ((pageEnd - pageStart + 1) < maxVisiblePages) {
    pageStart = Math.max(1, pageEnd - maxVisiblePages + 1);
  }

  if (pageStart > 1) {
    paginationContainer.appendChild(createPaginationButton('1', 1));
    if (pageStart > 2) {
      const leadEllipsis = document.createElement('span');
      leadEllipsis.className = 'pagination-ellipsis';
      leadEllipsis.textContent = '...';
      paginationContainer.appendChild(leadEllipsis);
    }
  }

  for (let page = pageStart; page <= pageEnd; page += 1) {
    paginationContainer.appendChild(
      createPaginationButton(String(page), page, { active: page === currentDirectoryPage })
    );
  }

  if (pageEnd < totalPages) {
    if (pageEnd < totalPages - 1) {
      const trailEllipsis = document.createElement('span');
      trailEllipsis.className = 'pagination-ellipsis';
      trailEllipsis.textContent = '...';
      paginationContainer.appendChild(trailEllipsis);
    }
    paginationContainer.appendChild(createPaginationButton(String(totalPages), totalPages));
  }

  paginationContainer.appendChild(
    createPaginationButton('Next', currentDirectoryPage + 1, { disabled: currentDirectoryPage >= totalPages })
  );
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  if (Array.isArray(value)) {
    value = value.filter(v => v !== null && v !== undefined && String(v).trim() !== '').join('; ');
  } else if (value instanceof Date) {
    value = value.toISOString();
  } else if (typeof value === 'object') {
    value = JSON.stringify(value);
  }

  const text = String(value);
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function getAlumniExportValue(record, field) {
  if (!record) return '';

  switch (field) {
    case 'linkedin_url':
      return record.linkedin_url || record.linkedin || '';
    case 'degree':
      return record.degree_raw || record.full_degree || record.degree || '';
    case 'major':
      return record.major_raw || record.full_major || record.major || '';
    case 'working_while_studying':
      if (record.working_while_studying === true) return 'yes';
      if (record.working_while_studying === false) return 'no';
      return record.working_while_studying || '';
    case 'title':
      return record.title || record.current_job_title || record.role || '';
    default:
      return record[field] ?? '';
  }
}

function buildAlumniCsv(records, fields) {
  const header = fields.map(csvEscape).join(',');
  const rows = (records || []).map(record =>
    fields.map(field => csvEscape(getAlumniExportValue(record, field))).join(',')
  );
  return [header, ...rows].join('\r\n');
}

function buildCsvExportFilename() {
  const date = new Date().toISOString().slice(0, 10);
  return `unt-alumni-export-${date}.csv`;
}

function downloadCsv(csvText, filename) {
  const blob = new Blob([csvText], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.style.display = 'none';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

function setupCsvExport() {
  const openBtn = document.getElementById('exportCsvBtn');
  const modal = document.getElementById('csvExportModal');
  const fieldList = document.getElementById('csvFieldList');
  const selectedCount = document.getElementById('csvSelectedCount');
  const validation = document.getElementById('csvExportValidation');
  const selectAllBtn = document.getElementById('csvSelectAll');
  const clearAllBtn = document.getElementById('csvClearAll');
  const downloadBtn = document.getElementById('downloadCsvExport');
  const closeBtn = document.getElementById('closeCsvExportModal');
  const cancelBtn = document.getElementById('cancelCsvExport');

  if (!openBtn || !modal || !fieldList || !downloadBtn) return;

  const getCheckboxes = () => Array.from(fieldList.querySelectorAll('input[name="csvExportField"]'));

  const setValidation = message => {
    if (validation) validation.textContent = message || '';
  };

  const getSelectedFields = () => {
    const checked = new Set(getCheckboxes().filter(input => input.checked).map(input => input.value));
    return ALUMNI_EXPORT_FIELDS.filter(field => checked.has(field));
  };

  const updateSelectedCount = () => {
    if (!selectedCount) return;
    const selectedTotal = getSelectedFields().length;
    selectedCount.textContent = `${selectedTotal} of ${ALUMNI_EXPORT_FIELDS.length} selected`;
  };

  const renderFieldOptions = () => {
    if (fieldList.childElementCount) return;

    ALUMNI_EXPORT_FIELDS.forEach(field => {
      const label = document.createElement('label');
      label.className = 'check csv-export-field';

      const input = document.createElement('input');
      input.type = 'checkbox';
      input.name = 'csvExportField';
      input.value = field;
      input.checked = true;

      const text = document.createElement('span');
      text.textContent = field;

      label.appendChild(input);
      label.appendChild(text);
      fieldList.appendChild(label);
    });
  };

  const openModal = () => {
    renderFieldOptions();
    setValidation('');
    updateSelectedCount();
    modal.style.display = 'block';
    if (closeBtn) closeBtn.focus();
  };

  const closeModal = () => {
    modal.style.display = 'none';
    setValidation('');
    openBtn.focus();
  };

  openBtn.addEventListener('click', openModal);
  if (closeBtn) closeBtn.addEventListener('click', closeModal);
  if (cancelBtn) cancelBtn.addEventListener('click', closeModal);
  const overlay = modal.querySelector('.modal-overlay');
  if (overlay) overlay.addEventListener('click', closeModal);

  if (selectAllBtn) {
    selectAllBtn.addEventListener('click', () => {
      getCheckboxes().forEach(input => {
        input.checked = true;
      });
      setValidation('');
      updateSelectedCount();
    });
  }

  if (clearAllBtn) {
    clearAllBtn.addEventListener('click', () => {
      getCheckboxes().forEach(input => {
        input.checked = false;
      });
      updateSelectedCount();
    });
  }

  fieldList.addEventListener('change', event => {
    if (event.target && event.target.matches('input[name="csvExportField"]')) {
      if (event.target.checked) setValidation('');
      updateSelectedCount();
    }
  });

  modal.addEventListener('keydown', event => {
    if (event.key === 'Escape') {
      closeModal();
    }
  });

  downloadBtn.addEventListener('click', () => {
    const fields = getSelectedFields();
    if (!fields.length) {
      setValidation('Select at least one field before exporting.');
      return;
    }
    if (isLoadingAlumni) {
      setValidation('Alumni are still loading. Try again when the list finishes.');
      return;
    }

    const csv = buildAlumniCsv(loadedAlumni, fields);
    downloadCsv(csv, buildCsvExportFilename());
    closeModal();
  });
}

function getCanonicalRoleTitle(value) {
  const title = (value || '').trim();
  if (!title) return '';
  const withoutLevelSuffix = title.replace(/\s+(?:level\s*)?(?:i{1,5}|[1-9])$/i, '').trim();
  const withoutSeniority = withoutLevelSuffix.replace(/^(?:senior|sr\.?)\s+/i, '').trim();
  const canonicalTitle = withoutSeniority || withoutLevelSuffix || title;
  const low = canonicalTitle.toLowerCase().replace(/\s+/g, ' ');

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
  if (low === 'data owner') {
    return 'Data Analyst';
  }
  if (low === 'software developer' || low === 'software dev') {
    return 'Software Engineer';
  }
  return canonicalTitle;
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
  const majors = Array.from(new Set(list.map(x => x.discipline).filter(Boolean)))
    .filter(m => APPROVED_ENGINEERING_DISCIPLINES.includes(m))
    .sort();
  const stdMajors = Array.from(new Set(
    list.flatMap(x => (x.standardized_majors || []).filter(Boolean))
  )).filter(m => m !== 'Other').sort();
  const degrees = ['Bachelors', 'Masters', 'PhD'].filter(level =>
    list.some(x => x.degree === level && isValid(level))
  );

  const selectedLocations = new Set(Array.from(document.querySelectorAll('input[name="location"]:checked')).map(i => i.value));
  const selectedRoles = new Set(Array.from(document.querySelectorAll('input[name="role"]:checked')).map(i => i.value));
  const selectedCompanies = new Set(Array.from(document.querySelectorAll('input[name="company"]:checked')).map(i => i.value));
  const selectedMajors = new Set(Array.from(document.querySelectorAll('input[name="major"]:checked')).map(i => i.value));
  const selectedStdMajors = new Set(Array.from(document.querySelectorAll('input[name="standardized_major"]:checked')).map(i => i.value));
  const selectedDegrees = new Set(Array.from(document.querySelectorAll('input[name="degree"]:checked')).map(i => i.value));
  const locChecks = document.getElementById('locChecks');
  const roleChecks = document.getElementById('roleChecks');
  const companyChecks = document.getElementById('companyChecks');
  const majorChecks = document.getElementById('majorChecks');
  const stdMajorChecks = document.getElementById('stdMajorChecks');
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

  if (stdMajorChecks) {
    stdMajorChecks.innerHTML = '';
    stdMajors.forEach(v => {
      const label = document.createElement('label');
      label.className = 'check';
      label.innerHTML = `<input type="checkbox" name="standardized_major" value="${v}" ${selectedStdMajors.has(v) ? 'checked' : ''} /> <span>${v}</span>`;
      stdMajorChecks.appendChild(label);
    });
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
  const sourceName = String(a.name || '').trim();
  const nameParts = sourceName ? sourceName.split(/\s+/) : [];
  const first = a.first || a.first_name || nameParts[0] || '';
  const last = a.last || a.last_name || (nameParts.length > 1 ? nameParts.slice(1).join(' ') : '');

  return {
    id: a.id,
    first,
    last,
    name: sourceName || `${first} ${last}`.trim(),
    role: a.role || a.current_job_title || a.title || '',
    title: a.title || a.current_job_title || a.role || '',
    current_job_title: a.current_job_title || a.title || a.role || '',
    normalized_title: a.normalized_title || '',
    company: a.company || '',
    class: a.class || a.grad_year || '',
    location: a.location || '',
    headline: a.headline || '',
    linkedin: a.linkedin || a.linkedin_url || '',
    linkedin_url: a.linkedin_url || a.linkedin || '',
    degree: a.degree || '',
    degree_raw: a.degree_raw || a.full_degree || '',
    full_degree: a.full_degree || '',
    full_major: a.full_major || '',
    major: a.major || '',
    major_raw: a.major_raw || '',
    standardized_majors: a.standardized_majors || [],
    discipline: a.discipline || '',
    school: a.school || '',
    school_start: a.school_start || a.school_start_date || '',
    school_start_date: a.school_start_date || a.school_start || '',
    grad_year: a.grad_year || a.class || '',
    school2: a.school2 || '',
    degree2: a.degree2 || '',
    major2: a.major2 || '',
    school3: a.school3 || '',
    degree3: a.degree3 || '',
    major3: a.major3 || '',
    updated_at: a.updated_at || '',
    working_while_studying: a.working_while_studying !== undefined ? a.working_while_studying : null,
    job_employment_type: a.job_employment_type || '',
    job_start: a.job_start || a.job_start_date || '',
    job_start_date: a.job_start_date || a.job_start || '',
    job_end: a.job_end || a.job_end_date || '',
    job_end_date: a.job_end_date || a.job_end || '',
    exp_2_title: a.exp_2_title || a.exp2_title || '',
    exp2_title: a.exp2_title || a.exp_2_title || '',
    exp_2_company: a.exp_2_company || a.exp2_company || '',
    exp2_company: a.exp2_company || a.exp_2_company || '',
    exp_2_dates: a.exp_2_dates || a.exp2_dates || '',
    exp2_dates: a.exp2_dates || a.exp_2_dates || '',
    exp_2_employment_type: a.exp_2_employment_type || a.exp2_employment_type || '',
    exp2_employment_type: a.exp2_employment_type || a.exp_2_employment_type || '',
    exp_3_title: a.exp_3_title || a.exp3_title || '',
    exp3_title: a.exp3_title || a.exp_3_title || '',
    exp_3_company: a.exp_3_company || a.exp3_company || '',
    exp3_company: a.exp3_company || a.exp_3_company || '',
    exp_3_dates: a.exp_3_dates || a.exp3_dates || '',
    exp3_dates: a.exp3_dates || a.exp_3_dates || '',
    exp_3_employment_type: a.exp_3_employment_type || a.exp3_employment_type || '',
    exp3_employment_type: a.exp3_employment_type || a.exp_3_employment_type || '',
    unt_alumni_status: a.unt_alumni_status || 'unknown',
    seniority_level: a.seniority_level || '',
    seniority_bucket: a.seniority_bucket || '',
    relevant_experience_months: a.relevant_experience_months != null ? a.relevant_experience_months : null
  };
}

function collectQueryState() {
  const q = document.getElementById('q');
  const gradYearFromInput = document.getElementById('gradYearFrom');
  const gradYearToInput = document.getElementById('gradYearTo');
  const sortSelect = document.getElementById('sortSelect');
  const expMinInput = document.getElementById('expMin');
  const expMaxInput = document.getElementById('expMax');
  const includeUnknownExperienceInput = document.getElementById('includeUnknownExperience');

  const term = q ? q.value.trim() : '';
  const loc = Array.from(document.querySelectorAll('input[name="location"]:checked')).map(i => i.value);
  const role = Array.from(document.querySelectorAll('input[name="role"]:checked')).map(i => i.value);
  const company = Array.from(document.querySelectorAll('input[name="company"]:checked')).map(i => i.value);
  const seniority = Array.from(document.querySelectorAll('input[name="seniority"]:checked')).map(i => i.value);
  const major = Array.from(document.querySelectorAll('input[name="major"]:checked')).map(i => i.value);
  const standardized_major = Array.from(document.querySelectorAll('input[name="standardized_major"]:checked')).map(i => i.value);
  const degree = Array.from(document.querySelectorAll('input[name="degree"]:checked')).map(i => i.value);
  const majorLogicInput = document.querySelector('input[name="majorLogic"]:checked');
  const majorLogic = majorLogicInput ? majorLogicInput.value : 'and';
  const gradYearFrom = gradYearFromInput && gradYearFromInput.value.trim() !== ''
    ? parseInt(gradYearFromInput.value, 10)
    : null;
  const gradYearTo = gradYearToInput && gradYearToInput.value.trim() !== ''
    ? parseInt(gradYearToInput.value, 10)
    : null;
  const wwsRadio = document.querySelector('input[name="workingWhileStudying"]:checked');
  const wws = wwsRadio ? wwsRadio.value : '';
  const untAlumniStatusRadio = document.querySelector('input[name="untAlumniStatus"]:checked');
  const untAlumniStatus = untAlumniStatusRadio ? untAlumniStatusRadio.value : '';

  // Experience range (in years from UI, converted to months for API)
  const expMinYears = expMinInput && expMinInput.value.trim() !== '' ? parseInt(expMinInput.value, 10) : null;
  const expMaxYears = expMaxInput && expMaxInput.value.trim() !== '' ? parseInt(expMaxInput.value, 10) : null;
  const expMin = Number.isFinite(expMinYears) && expMinYears >= 0 ? expMinYears * 12 : null;
  const expMax = Number.isFinite(expMaxYears) && expMaxYears >= 0 ? expMaxYears * 12 : null;
  const includeUnknownExperience = Boolean(includeUnknownExperienceInput && includeUnknownExperienceInput.checked);

  const sortValue = sortSelect ? (sortSelect.value || '') : '';
  const bookmarkedOnly = sortValue === 'bookmarked';
  const sort = bookmarkedOnly ? 'name' : (sortValue || 'name');

  return {
    term,
    loc,
    role,
    company,
    seniority,
    major,
    standardized_major,
    degree,
    majorLogic,
    gradYearFrom,
    gradYearTo,
    wws,
    untAlumniStatus,
    expMin,
    expMax,
    includeUnknownExperience,
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
  queryState.loc.forEach(v => params.append('location', v));
  queryState.role.forEach(v => params.append('role', v));
  queryState.company.forEach(v => params.append('company', v));
  queryState.seniority.forEach(v => params.append('seniority', v));
  queryState.major.forEach(v => params.append('major', v));
  queryState.standardized_major.forEach(v => params.append('standardized_major', v));
  if (queryState.major.length && queryState.standardized_major.length) {
    params.set('major_logic', queryState.majorLogic || 'and');
  }
  queryState.degree.forEach(v => params.append('degree', v));
  if (queryState.gradYearFrom != null) params.set('grad_year_from', String(queryState.gradYearFrom));
  if (queryState.gradYearTo != null) params.set('grad_year_to', String(queryState.gradYearTo));
  if (queryState.wws) params.set('working_while_studying', queryState.wws);
  if (queryState.untAlumniStatus) params.set('unt_alumni_status', queryState.untAlumniStatus);
  if (queryState.sort) params.set('sort', queryState.sort);
  params.set('direction', queryState.direction);
  if (queryState.bookmarkedOnly) params.set('bookmarked_only', '1');
  if (queryState.expMin != null) params.set('exp_min', String(queryState.expMin));
  if (queryState.expMax != null) params.set('exp_max', String(queryState.expMax));
  if (queryState.includeUnknownExperience) params.set('include_unknown_experience', '1');

  return params;
}

async function fetchAlumniPage({ reset = false, initializeFilters = false } = {}) {
  const requestToken = ++activeRequestToken;

  if (reset) {
    loadedAlumni = [];
    totalAlumniCount = 0;
    currentDirectoryPage = 1;
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

  const effectiveSort = sortSelect.value || 'name';
  let text = "";
  if (effectiveSort === 'name') {
    text = sortDirection === 'asc' ? "(A -> Z)" : "(Z -> A)";
  } else if (effectiveSort === 'year') {
    text = sortDirection === 'asc' ? "(Oldest -> Newest)" : "(Newest -> Oldest)";
  } else if (effectiveSort === 'updated') {
    text = sortDirection === 'asc' ? "(Oldest -> Newest)" : "(Newest -> Oldest)";
  }
  sortLabel.textContent = text;
}

function setupFiltering() {
  const q = document.getElementById('q');
  const gradYearFromInput = document.getElementById('gradYearFrom');
  const gradYearToInput = document.getElementById('gradYearTo');
  const gradRangeWarning = document.getElementById('gradRangeWarning');
  const sortSelect = document.getElementById('sortSelect');
  const sortReverseBtn = document.getElementById('sortReverseBtn');
  const clearBtn = document.getElementById('clear-filters');
  const expMinInput = document.getElementById('expMin');
  const expMaxInput = document.getElementById('expMax');
  const expRangeWarning = document.getElementById('expRangeWarning');
  const includeUnknownExperience = document.getElementById('includeUnknownExperience');
  const majorLogicHint = document.getElementById('majorLogicHint');

  let searchDebounce = null;
  const setRangeWarning = (el, message) => {
    if (!el) return;
    el.textContent = message || '';
  };

  const enforceIntegerOnly = (input, warningEl) => {
    if (!input) return;
    input.addEventListener('beforeinput', (e) => {
      if (e.data == null) return;
      if (e.data === '.') {
        e.preventDefault();
        setRangeWarning(warningEl, 'Integer values only.');
        return;
      }
      if (!/^\d+$/.test(e.data)) {
        e.preventDefault();
        setRangeWarning(warningEl, 'Integer values only.');
      }
    });

    input.addEventListener('input', () => {
      const original = input.value;
      const sanitized = original.replace(/[^\d]/g, '');
      if (sanitized !== original) {
        input.value = sanitized;
        setRangeWarning(warningEl, 'Integer values only.');
      } else if (warningEl && warningEl.textContent === 'Integer values only.') {
        setRangeWarning(warningEl, '');
      }
    });
  };

  const validateExperienceRange = () => {
    const minVal = expMinInput && expMinInput.value.trim() !== '' ? parseInt(expMinInput.value, 10) : null;
    const maxVal = expMaxInput && expMaxInput.value.trim() !== '' ? parseInt(expMaxInput.value, 10) : null;
    if (minVal != null && maxVal != null && minVal > maxVal) {
      setRangeWarning(expRangeWarning, 'Min cannot be greater than max.');
      return false;
    }
    if (expRangeWarning && expRangeWarning.textContent === 'Min cannot be greater than max.') {
      setRangeWarning(expRangeWarning, '');
    }
    return true;
  };

  const validateGraduationYearRange = () => {
    const minVal = gradYearFromInput && gradYearFromInput.value.trim() !== '' ? parseInt(gradYearFromInput.value, 10) : null;
    const maxVal = gradYearToInput && gradYearToInput.value.trim() !== '' ? parseInt(gradYearToInput.value, 10) : null;
    if (minVal != null && maxVal != null && minVal > maxVal) {
      setRangeWarning(gradRangeWarning, 'From year cannot be greater than to year.');
      return false;
    }
    if (gradRangeWarning && gradRangeWarning.textContent === 'From year cannot be greater than to year.') {
      setRangeWarning(gradRangeWarning, '');
    }
    return true;
  };

  enforceIntegerOnly(expMinInput, expRangeWarning);
  enforceIntegerOnly(expMaxInput, expRangeWarning);
  enforceIntegerOnly(gradYearFromInput, gradRangeWarning);
  enforceIntegerOnly(gradYearToInput, gradRangeWarning);

  const applyFilters = async () => {
    if (!validateExperienceRange() || !validateGraduationYearRange()) {
      return;
    }
    await fetchAlumniPage({ reset: true });
  };

  const updateMajorLogicHint = () => {
    if (!majorLogicHint) return;
    const selected = document.querySelector('input[name="majorLogic"]:checked');
    const logic = selected ? selected.value : 'and';
    majorLogicHint.textContent = logic === 'or'
      ? 'Matching results from either filter type.'
      : 'Matching results from both filter types.';
  };

  if (clearBtn) {
    clearBtn.addEventListener('click', async () => {
      document.querySelectorAll('input[name="location"], input[name="role"], input[name="company"], input[name="seniority"], input[name="major"], input[name="standardized_major"], input[name="degree"]').forEach(cb => cb.checked = false);
      const majorLogicAnd = document.querySelector('input[name="majorLogic"][value="and"]');
      if (majorLogicAnd) majorLogicAnd.checked = true;
      if (gradYearFromInput) gradYearFromInput.value = '';
      if (gradYearToInput) gradYearToInput.value = '';
      if (q) q.value = '';
      const wwsAll = document.querySelector('input[name="workingWhileStudying"][value=""]');
      if (wwsAll) wwsAll.checked = true;
      const untStatusAll = document.querySelector('input[name="untAlumniStatus"][value=""]');
      if (untStatusAll) untStatusAll.checked = true;
      const expMinInput = document.getElementById('expMin');
      const expMaxInput = document.getElementById('expMax');
      if (expMinInput) expMinInput.value = '';
      if (expMaxInput) expMaxInput.value = '';
      if (includeUnknownExperience) includeUnknownExperience.checked = false;
      setRangeWarning(gradRangeWarning, '');
      setRangeWarning(expRangeWarning, '');
      updateMajorLogicHint();
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
    if (e.target.matches('input[name="location"], input[name="role"], input[name="company"], input[name="seniority"], input[name="major"], input[name="standardized_major"], input[name="degree"], input[name="majorLogic"], #gradYearFrom, #gradYearTo, input[name="workingWhileStudying"], input[name="untAlumniStatus"], #expMin, #expMax, #includeUnknownExperience')) {
      if (e.target.matches('input[name="majorLogic"]')) {
        updateMajorLogicHint();
      }
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
  updateMajorLogicHint();
  updateSortLabel();
}

// Initialize and auto-load all alumni rows for the active query in backend-sized chunks.
(async function init() {
  notesModal.create();
  setupCsvExport();
  setupFiltering();
  await fetchAlumniPage({ reset: true, initializeFilters: true });
})();
