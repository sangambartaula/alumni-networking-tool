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
let currentPage = 1;
const itemsPerPage = 20;

const listContainer = document.getElementById('list');
const count = document.getElementById('count');

// ===== STATS BANNER UPDATE FUNCTION =====
function updateStatsBanner(alumniData) {
  if (!alumniData || alumniData.length === 0) {
    return;
  }

  // Calculate total alumni
  const totalAlumni = alumniData.length;

  // Calculate unique locations
  const uniqueLocations = new Set(alumniData.map(a => a.location).filter(loc => loc));
  const locationsCount = uniqueLocations.size;

  // Calculate bookmarked alumni - check interaction_type === 'bookmarked'
  const bookmarkedCount = Object.values(userInteractions).filter(interaction => interaction.interaction_type === 'bookmarked').length;

  // Calculate working while studying count
  const wwsCount = alumniData.filter(a => a.working_while_studying === true).length;

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
  const bookmarkedCount = Object.values(userInteractions).filter(interaction => interaction.interaction_type === 'bookmarked').length;
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
        // Update the button styling
        const notesBtn = document.querySelector(`[data-alumni-id="${this.currentAlumniId}"]`);
        // Update cache
        const hasNote = !!noteContent.trim();
        notesStatusCache[this.currentAlumniId] = hasNote;

        if (notesBtn) {
          if (hasNote) {
            notesBtn.classList.add('has-note');
          } else {
            notesBtn.classList.remove('has-note');
          }
        }
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

async function fetchNoteStatus(id, btn) {
  // Check cache first
  if (notesStatusCache[id] !== undefined) {
    if (notesStatusCache[id]) btn.classList.add('has-note');
    return;
  }

  try {
    const response = await fetch(`/api/notes/${id}`);
    const data = await response.json();
    const hasNote = data.success && data.note && data.note.note_content && data.note.note_content.trim().length > 0;

    // Update cache
    notesStatusCache[id] = hasNote;

    if (hasNote) {
      btn.classList.add('has-note');
    }
  } catch (error) {
    console.error('Error loading notes status:', error);
  }
}

/**
 * Lazy-loads note status as list items enter the viewport.
 * This prevents firing hundreds of API calls for the entire alumni list at once,
 * significantly improving initial load performance and reducing server load.
 */
const notesObserver = new IntersectionObserver((entries, observer) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      const btn = entry.target;
      const id = btn.dataset.alumniId;
      fetchNoteStatus(id, btn);
      observer.unobserve(btn);
    }
  });
});

async function loadUserInteractions() {
  try {
    const response = await fetch('/api/user-interactions');
    const data = await response.json();

    if (data.success) {
      // Convert interactions array to a map for easy lookup
      // Key: "alumni_id-interaction_type", Value: interaction data
      userInteractions = {};
      data.interactions.forEach(interaction => {
        const key = `${interaction.alumni_id}-${interaction.interaction_type}`;
        userInteractions[key] = interaction;
      });
      console.log('User interactions loaded:', userInteractions);
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

// Create profile list item element (horizontal row)
function createListItem(p) {
  const item = document.createElement('div');
  item.className = 'list-item';
  item.setAttribute('data-id', p.id);
  item.innerHTML = `
      <div class="list-main">
        <div class="list-details">
          <h3 class="name">${p.name}</h3>
          <p class="role">${p.role || p.headline || ''}</p>
          <div class="class">Class of ${p.class}${p.location ? ' · ' + p.location : ''}</div>
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
          <button class="btn star" type="button" title="Bookmark this alumni">⭐</button>
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

  // Bookmark button action - TOGGLE between ⭐ and ★
  const starBtn = item.querySelector('.btn.star');
  if (hasInteraction(p.id, 'bookmarked')) {
    item.classList.add('bookmarked');
    starBtn.textContent = '★';
  }
  starBtn.addEventListener('click', async () => {
    const isCurrentlyBookmarked = item.classList.contains('bookmarked');
    if (isCurrentlyBookmarked) {
      const success = await removeInteraction(p.id, 'bookmarked');
      if (success) {
        item.classList.remove('bookmarked');
        starBtn.textContent = '⭐';
        updateBookmarkCount(); // Update banner count
      }
    } else {
      const success = await saveInteraction(p.id, 'bookmarked');
      if (success) {
        item.classList.add('bookmarked');
        starBtn.textContent = '★';
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
  notesBtn.addEventListener('click', async () => {
    const alumniId = parseInt(notesBtn.dataset.alumniId);
    const alumniName = notesBtn.dataset.alumniName;
    notesModal.open(alumniId, alumniName);
  });

  // Load notes status lazily when visible
  notesObserver.observe(notesBtn);

  return item;
}

function renderProfiles(list) {
  // Get paginated subset
  const paginatedList = getPaginated(list);

  if (listContainer) {
    listContainer.innerHTML = '';
    paginatedList.forEach(p => listContainer.appendChild(createListItem(p)));
  }
  if (count) count.textContent = `(${list.length} total, showing ${paginatedList.length})`;
  // Render pagination controls
  renderPagination(list);
}

// Pagination: Get paginated subset of profiles
function getPaginated(list) {
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  return list.slice(startIndex, endIndex);
}

// Render pagination controls
function renderPagination(fullList) {
  const paginationContainer = document.getElementById('pagination');
  if (!paginationContainer) return;

  const totalPages = Math.ceil(fullList.length / itemsPerPage);

  // If only one page or no results, hide pagination
  if (totalPages <= 1) {
    paginationContainer.innerHTML = '';
    paginationContainer.style.display = 'none';
    return;
  }

  paginationContainer.style.display = 'flex';
  paginationContainer.innerHTML = '';

  // Previous button
  const prevBtn = document.createElement('button');
  prevBtn.className = 'pagination-btn';
  prevBtn.textContent = '← Previous';
  prevBtn.disabled = currentPage === 1;
  prevBtn.addEventListener('click', () => {
    if (currentPage > 1) {
      currentPage--;
      applyFiltersAndSort();
    }
  });
  paginationContainer.appendChild(prevBtn);

  // Page number buttons
  const maxVisiblePages = 7;
  let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
  let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);

  // Adjust start if we're near the end
  if (endPage - startPage < maxVisiblePages - 1) {
    startPage = Math.max(1, endPage - maxVisiblePages + 1);
  }

  // First page + ellipsis if needed
  if (startPage > 1) {
    const firstBtn = createPageButton(1, fullList);
    paginationContainer.appendChild(firstBtn);
    if (startPage > 2) {
      const ellipsis = document.createElement('span');
      ellipsis.className = 'pagination-ellipsis';
      ellipsis.textContent = '...';
      paginationContainer.appendChild(ellipsis);
    }
  }

  // Visible page buttons
  for (let i = startPage; i <= endPage; i++) {
    const pageBtn = createPageButton(i, fullList);
    paginationContainer.appendChild(pageBtn);
  }

  // Last page + ellipsis if needed
  if (endPage < totalPages) {
    if (endPage < totalPages - 1) {
      const ellipsis = document.createElement('span');
      ellipsis.className = 'pagination-ellipsis';
      ellipsis.textContent = '...';
      paginationContainer.appendChild(ellipsis);
    }
    const lastBtn = createPageButton(totalPages, fullList);
    paginationContainer.appendChild(lastBtn);
  }

  // Next button
  const nextBtn = document.createElement('button');
  nextBtn.className = 'pagination-btn';
  nextBtn.textContent = 'Next →';
  nextBtn.disabled = currentPage === totalPages;
  nextBtn.addEventListener('click', () => {
    if (currentPage < totalPages) {
      currentPage++;
      applyFiltersAndSort();
    }
  });
  paginationContainer.appendChild(nextBtn);
}

// Helper: Create a page number button
function createPageButton(pageNum, fullList) {
  const btn = document.createElement('button');
  btn.className = 'pagination-btn page-number';
  if (pageNum === currentPage) {
    btn.classList.add('active');
  }
  btn.textContent = pageNum;
  btn.addEventListener('click', () => {
    currentPage = pageNum;
    applyFiltersAndSort();
  });
  return btn;
}

/**
 * Dynamically builds the multi-select filter UI based on the actual 
 * data returned from the API. This ensures that only relevant options 
 * (e.g., existing companies or locations) are shown to the user.
 */
function populateFilters(list) {
  // Helper to filter out bad values
  const isValid = val => val && !['Unknown', 'Not Found', 'N/A'].includes(val);

  // Helper: Normalize company names for filtering (duplicate definition for scope access if needed, or move helper out)
  function getNormalizedCompany(name) {
    if (!name) return "";
    if (name.includes("Dallas")) return name;
    if (name.includes("University of North Texas") || name.startsWith("UNT ") || name === "UNT" || name.includes(" UNT ") || name.endsWith(" UNT")) {
      return "University of North Texas";
    }
    return name;
  }

  const locations = Array.from(new Set(list.map(x => x.location).filter(isValid))).sort();
  const roles = Array.from(new Set(list.map(x => x.normalized_title || x.role).filter(isValid))).sort();
  // Normalize companies before creating the Set
  const companies = Array.from(new Set(list.map(x => getNormalizedCompany(x.company)).filter(isValid))).sort();
  // Filter majors to only show approved engineering disciplines (which excludes Unknown now)
  const majors = Array.from(new Set(list.map(x => x.major).filter(Boolean)))
    .filter(m => APPROVED_ENGINEERING_DISCIPLINES.includes(m))
    .sort();
  const years = Array.from(new Set(list.map(x => x.class).filter(Boolean))).sort((a, b) => b - a);
  // Fixed order for degree levels
  const degrees = ['Undergraduate', 'Graduate', 'PhD'].filter(level =>
    list.some(x => x.degree === level && isValid(level))
  );

  console.log('Degree values found:', degrees);
  console.log('Sample alumni degrees:', list.slice(0, 5).map(x => ({ name: x.name, degree: x.degree })));

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
      label.innerHTML = `<input type="checkbox" name="location" value="${v}" /> <span>${v}</span>`;
      locChecks.appendChild(label);
    });
  }

  if (roleChecks) {
    roleChecks.innerHTML = '';
    roles.forEach(v => {
      const label = document.createElement('label');
      label.className = 'check';
      label.innerHTML = `<input type="checkbox" name="role" value="${v}" /> <span>${v}</span>`;
      roleChecks.appendChild(label);
    });
  }

  if (companyChecks) {
    companyChecks.innerHTML = '';
    companies.forEach(v => {
      const label = document.createElement('label');
      label.className = 'check';
      label.innerHTML = `<input type="checkbox" name="company" value="${v}" /> <span>${v}</span>`;
      companyChecks.appendChild(label);
    });
  }

  if (majorChecks) {
    majorChecks.innerHTML = '';
    majors.forEach(v => {
      const label = document.createElement('label');
      label.className = 'check';
      label.innerHTML = `<input type="checkbox" name="major" value="${v}" /> <span>${v}</span>`;
      majorChecks.appendChild(label);
    });
  }

  if (gradSelect) {
    gradSelect.innerHTML = '<option value=\"\">All years</option>';
    years.forEach(y => {
      const opt = document.createElement('option');
      opt.value = y;
      opt.textContent = y;
      gradSelect.appendChild(opt);
    });
  }

  if (degreeChecks) {
    degreeChecks.innerHTML = '';
    degrees.forEach(v => {
      const label = document.createElement('label');
      label.className = 'check';
      label.innerHTML = `<input type="checkbox" name="degree" value="${v}" /> <span>${v}</span>`;
      degreeChecks.appendChild(label);
    });
  }
}

// Filtering behavior
function setupFiltering(list) {
  const q = document.getElementById('q');
  const gradSelect = document.getElementById('gradSelect');
  const sortSelect = document.getElementById('sortSelect');
  const sortReverseBtn = document.getElementById('sortReverseBtn');
  const sortLabel = document.getElementById('sortLabel');
  let sortDirection = 'desc'; // 'asc' or 'desc'

  const clearBtn = document.getElementById('clear-filters');
  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      document.querySelectorAll('input[name="location"], input[name="role"], input[name="company"], input[name="major"], input[name="degree"]').forEach(cb => cb.checked = false);
      if (gradSelect) gradSelect.value = '';
      if (q) q.value = '';
      // Reset working while studying radio to 'All'
      const wwsAll = document.querySelector('input[name="workingWhileStudying"][value=""]');
      if (wwsAll) wwsAll.checked = true;
      currentPage = 1; // Reset to page 1 when clearing filters
      apply();
    });
  }

  function getFilters() {
    const term = q ? q.value.trim().toLowerCase() : '';
    const loc = Array.from(document.querySelectorAll('input[name="location"]:checked')).map(i => i.value);
    const role = Array.from(document.querySelectorAll('input[name="role"]:checked')).map(i => i.value);
    const company = Array.from(document.querySelectorAll('input[name="company"]:checked')).map(i => i.value);
    const major = Array.from(document.querySelectorAll('input[name="major"]:checked')).map(i => i.value);
    const degree = Array.from(document.querySelectorAll('input[name="degree"]:checked')).map(i => i.value);
    const year = gradSelect ? gradSelect.value : '';
    const wwsRadio = document.querySelector('input[name="workingWhileStudying"]:checked');
    const wws = wwsRadio ? wwsRadio.value : '';
    return { term, loc, role, company, major, degree, year, wws };
  }

  function getSorted(listToSort) {
    if (!sortSelect) return listToSort;
    // If no sort selected, treat as 'Default'
    const value = sortSelect.value || "";
    let sorted = [...listToSort];

    // Filter first if needed (bookmarked is special, acts as filter here)
    if (value === "bookmarked") {
      sorted = sorted.filter(a => hasInteraction(a.id, 'bookmarked'));
    } else if (value === "name") {
      sorted.sort((a, b) => {
        const valA = (a.name || "").toLowerCase();
        const valB = (b.name || "").toLowerCase();
        return sortDirection === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
      });
    } else if (value === "year") {
      sorted.sort((a, b) => {
        const valA = parseInt(a.class) || 0;
        const valB = parseInt(b.class) || 0;
        return sortDirection === 'asc' ? valA - valB : valB - valA;
      });
    } else if (value === "updated") {
      sorted.sort((a, b) => {
        const valA = new Date(a.updated_at || 0).getTime();
        const valB = new Date(b.updated_at || 0).getTime();
        return sortDirection === 'asc' ? valA - valB : valB - valA;
      });
    }

    return sorted;
  }

  function updateSortLabel() {
    if (!sortLabel || !sortSelect) return;
    const val = sortSelect.value;
    let text = "";
    if (val === 'name') {
      text = sortDirection === 'asc' ? "(A → Z)" : "(Z → A)";
    } else if (val === 'year') {
      text = sortDirection === 'asc' ? "(Oldest → Newest)" : "(Newest → Oldest)";
    } else if (val === 'updated') {
      text = sortDirection === 'asc' ? "(Oldest → Newest)" : "(Newest → Oldest)";
    }
    sortLabel.textContent = text;
  }
  function getNormalizedCompany(name) {
    if (!name) return "";
    // Check for "Dallas" exclusion first
    if (name.includes("Dallas")) return name;

    // Check for UNT variations
    if (name.includes("University of North Texas") || name.startsWith("UNT ") || name === "UNT" || name.includes(" UNT ") || name.endsWith(" UNT")) {
      return "University of North Texas";
    }
    return name;
  }

  /**
   * The core search/filter engine.
   * Logic:
   * 1. Collects state from all checkboxes, inputs, and sort toggles.
   * 2. Filters the master list based on combined criteria (AND logic).
   * 3. Sorts the result.
   * 4. Triggers re-render of paginated cards.
   */
  function apply() {
    const f = getFilters();
    const filtered = list.filter(a => {
      const t = `${a.name} ${a.role} ${a.company} ${a.headline}`.toLowerCase();
      const matchTerm = !f.term || t.includes(f.term);
      const matchLoc = !f.loc.length || f.loc.includes(a.location);
      const matchRole = !f.role.length || f.role.includes(a.normalized_title || a.role);

      // Normalize company for matching
      const normCompany = getNormalizedCompany(a.company);
      const matchCompany = !f.company.length || f.company.includes(normCompany);

      const matchMajor = !f.major.length || f.major.includes(a.major);
      const matchDegree = !f.degree.length || f.degree.includes(a.degree);
      const matchYear = !f.year || String(a.class) === String(f.year);
      // Working while studying: '' = all, 'yes' = true, 'no' = false/null
      let matchWws = true;
      if (f.wws === 'yes') matchWws = a.working_while_studying === true;
      else if (f.wws === 'no') matchWws = a.working_while_studying === false || a.working_while_studying === null;
      return matchTerm && matchLoc && matchRole && matchCompany && matchMajor && matchDegree && matchYear && matchWws;
    });
    const sortedFiltered = getSorted(filtered);
    const paginated = getPaginated(sortedFiltered);
    renderProfiles(sortedFiltered); // Pass full list for count and pagination

    // Render only paginated items
    if (listContainer) {
      listContainer.innerHTML = '';
      paginated.forEach(p => listContainer.appendChild(createListItem(p)));
    }
    if (count) count.textContent = `(${sortedFiltered.length})`;
  }

  // Store reference to apply function globally
  window.applyFiltersAndSort = apply;

  if (q) q.addEventListener('input', () => {
    currentPage = 1; // Reset to page 1 on search
    apply();
  });
  document.addEventListener('change', (e) => {
    if (e.target.matches('input[name="location"], input[name="role"], input[name="company"], input[name="major"], input[name="degree"], #gradSelect, input[name="workingWhileStudying"]')) {
      currentPage = 1; // Reset to page 1 on filter change
      apply();
    }
  });
  if (sortSelect) sortSelect.addEventListener('change', () => {
    currentPage = 1; // Reset to page 1 on sort change
    // Reset direction to default for the new sort type if desired,
    // or keep current direction. Let's keep current direction or default to desc for consistency?
    // User requested "reverse the sort method", implies toggle.
    // Let's set a sensible default per type?
    // For now, preserve existing direction or just apply.
    updateSortLabel();
    apply();
  });

  if (sortReverseBtn) {
    sortReverseBtn.addEventListener('click', () => {
      sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
      // Optional: update icon rotation or visual indicator
      updateSortLabel();
      apply();
    });
  }
}

// Initialize: fetch alumni from backend and fall back to `fakeAlumni` if necessary
(async function init() {
  // Create notes modal first
  notesModal.create();

  // Attempt to fetch alumni from backend
  let alumniData = fakeAlumni;
  try {
    const resp = await fetch('/api/alumni?limit=10000');  // Get all alumni (up to 10,000)
    const data = await resp.json();

    if (data && data.success && Array.isArray(data.alumni) && data.alumni.length > 0) {
      // Map backend response to frontend expected fields
      alumniData = data.alumni.map(a => ({
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
        major: a.major || '',
        updated_at: a.updated_at || '',
        working_while_studying: a.working_while_studying !== undefined ? a.working_while_studying : null
      }));
      console.log('Loaded alumni from API, count=', alumniData.length);
    } else {
      console.log('No alumni returned from API, using fallback data');
    }
  } catch (err) {
    console.error('Error fetching alumni from API, using fallback data', err);
  }

  // Load interactions from backend first
  await loadUserInteractions();

  // Update stats banner with alumni data
  updateStatsBanner(alumniData);

  // Populate UI with the alumni data (from API or fallback)
  populateFilters(alumniData);
  setupFiltering(alumniData);
  // Show all profiles on first load (default sort)
  const sortSelect = document.getElementById('sortSelect');
  if (sortSelect) sortSelect.value = "";

  // Initial label update if needed (though default is empty)
  // But if we want to show label for default or if we set a value:
  // updateSortLabel(); // undefined function at this scope if not careful, but it's inside setupFiltering closure.
  // Actually setupFiltering is called above. We need to trigger label update there?
  // setupFiltering defines updateSortLabel inside. We can't call it here easily unless we expose it.
  // Or we just trigger a change event?
  // Simpler: Just rely on user interaction or ensure setupFiltering initializes it if value exists.
  // But wait, setupFiltering is a function that runs ONCE to setup listeners.
  // The inner functions are scoped.
  // To initialize label, we might need to expose it or run it inside logic.
  // Let's rely on the fact that default is "" (Default) which has no label text in my logic above.

  // Manually trigger initial rendering
  if (typeof renderProfiles === 'function') renderProfiles(alumniData);
})();

