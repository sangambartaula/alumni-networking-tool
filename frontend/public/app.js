// app.js
// Approved engineering disciplines (must match backend backfill_disciplines.py)
const APPROVED_ENGINEERING_DISCIPLINES = [
  'Computer Science & Engineering',
  'Electrical & Computer Engineering',
  'Biomedical Engineering',
  'Mechanical & Energy Engineering',
  'Materials Science & Engineering',
  'Construction & Engineering Management',
  'Engineering Technology'
];
// Fake alumni data (fallback). Backend will be queried first; if it fails we use this local list.
const fakeAlumni = [
  { id: 1, name: "Sachin Banjade", role: "Software Engineer", company: "Tech Solutions Inc.", class: 2020, location: "Dallas", linkedin: "https://www.linkedin.com/in/sachin-banjade-345339248/" },
  { id: 2, name: "Sangam Bartaula", role: "Data Scientist", company: "Data Insights Co.", class: 2021, location: "Austin", linkedin: "https://www.linkedin.com/in/sangambartaula/" },
  { id: 3, name: "Shrish Acharya", role: "Product Manager", company: "Innovate Labs", class: 2023, location: "Houston", linkedin: "https://www.linkedin.com/in/shrish-acharya-53b46932b/" },
  { id: 4, name: "Niranjan Paudel", role: "Cybersecurity Analyst", company: "SecureNet Systems", class: 2020, location: "Dallas", linkedin: "https://www.linkedin.com/in/niranjan-paudel-14a31a330/" },
  { id: 5, name: "Abishek Lamichhane", role: "Cloud Architect", company: "Global Cloud Services", class: 2022, location: "Remote", linkedin: "https://www.linkedin.com/in/abishek-lamichhane-b21ab6330/" },
];

// Store user interactions in memory
let userInteractions = {};

// Pagination state
let currentPage = 1;
const itemsPerPage = 20;

// DOM references
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

  // Update DOM elements
  const totalAlumniEl = document.getElementById('totalAlumni');
  const locationsCountEl = document.getElementById('locationsCount');
  const bookmarkedCountEl = document.getElementById('bookmarkedCount');

  if (totalAlumniEl) totalAlumniEl.textContent = totalAlumni;
  if (locationsCountEl) locationsCountEl.textContent = locationsCount;
  if (bookmarkedCountEl) bookmarkedCountEl.textContent = bookmarkedCount;
}

// Helper function to update just the bookmarked count in the banner
function updateBookmarkCount() {
  const bookmarkedCount = Object.values(userInteractions).filter(interaction => interaction.interaction_type === 'bookmarked').length;
  const bookmarkedCountEl = document.getElementById('bookmarkedCount');
  if (bookmarkedCountEl) bookmarkedCountEl.textContent = bookmarkedCount;
}

// ===== NOTES MODAL CLASS =====
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

// Initialize notes modal
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

// Load user interactions from backend
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

// Save interaction to backend
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

// Remove interaction from backend
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

// Render list to grid
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

// Populate filters (location, role, company, major, graduation, degree)
function populateFilters(list) {
  const locations = Array.from(new Set(list.map(x => x.location).filter(Boolean))).sort();
  const roles = Array.from(new Set(list.map(x => x.role).filter(Boolean))).sort();
  const companies = Array.from(new Set(list.map(x => x.company).filter(Boolean))).sort();
  // Filter majors to only show approved engineering disciplines
  const majors = Array.from(new Set(list.map(x => x.major).filter(Boolean)))
    .filter(m => APPROVED_ENGINEERING_DISCIPLINES.includes(m))
    .sort();
  const years = Array.from(new Set(list.map(x => x.class).filter(Boolean))).sort((a, b) => b - a);
  // Fixed order for degree levels
  const degrees = ['Undergraduate', 'Graduate', 'PhD'].filter(level =>
    list.some(x => x.degree === level)
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

  const clearBtn = document.getElementById('clear-filters');
  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      document.querySelectorAll('input[name="location"], input[name="role"], input[name="company"], input[name="major"], input[name="degree"]').forEach(cb => cb.checked = false);
      if (gradSelect) gradSelect.value = '';
      if (q) q.value = '';
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
    return { term, loc, role, company, major, degree, year };
  }

  function getSorted(listToSort) {
    if (!sortSelect) return listToSort;
    // If no sort selected, treat as 'Default'
    const value = sortSelect.value || "";
    let sorted = [...listToSort];
    if (value === "bookmarked") {
      sorted = sorted.filter(a => hasInteraction(a.id, 'bookmarked'));
    } else if (value === "name") {
      sorted.sort((a, b) => a.name.localeCompare(b.name));
    } else if (value === "year") {
      sorted.sort((a, b) => b.class - a.class);
    }
    return sorted;
  }

  function apply() {
    const f = getFilters();
    const filtered = list.filter(a => {
      const t = `${a.name} ${a.role} ${a.company} ${a.headline}`.toLowerCase();
      const matchTerm = !f.term || t.includes(f.term);
      const matchLoc = !f.loc.length || f.loc.includes(a.location);
      const matchRole = !f.role.length || f.role.includes(a.role);
      const matchCompany = !f.company.length || f.company.includes(a.company);
      const matchMajor = !f.major.length || f.major.includes(a.major);
      const matchDegree = !f.degree.length || f.degree.includes(a.degree);
      const matchYear = !f.year || String(a.class) === String(f.year);
      return matchTerm && matchLoc && matchRole && matchCompany && matchMajor && matchDegree && matchYear;
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
    if (e.target.matches('input[name="location"], input[name="role"], input[name="company"], input[name="major"], input[name="degree"], #gradSelect')) {
      currentPage = 1; // Reset to page 1 on filter change
      apply();
    }
  });
  if (sortSelect) sortSelect.addEventListener('change', () => {
    currentPage = 1; // Reset to page 1 on sort change
    apply();
  });
}

// Initialize: fetch alumni from backend and fall back to `fakeAlumni` if necessary
(async function init() {
  // Create notes modal first
  notesModal.create();

  // Attempt to fetch alumni from backend
  let alumniData = fakeAlumni;
  try {
    const resp = await fetch('/api/alumni?limit=500');
    const data = await resp.json();

    if (data && data.success && Array.isArray(data.alumni) && data.alumni.length > 0) {
      // Map backend response to frontend expected fields
      alumniData = data.alumni.map(a => ({
        id: a.id,
        name: a.name || '',
        role: a.role || '',
        company: a.company || '',
        class: a.class || '',
        location: a.location || '',
        headline: a.headline || '',
        linkedin: a.linkedin || '',
        degree: a.degree || '',
        major: a.major || ''
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
  // Manually trigger initial rendering
  if (typeof renderProfiles === 'function') renderProfiles(alumniData);
})();

