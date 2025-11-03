// app.js
// Fake alumni data (fallback). Backend will be queried first; if it fails we use this local list.
const fakeAlumni = [
  { id:1, name:"Sachin Banjade", role:"Software Engineer", company:"Tech Solutions Inc.", class:2020, location:"Dallas", linkedin:"https://www.linkedin.com/in/sachin-banjade-345339248/"},
  { id:2, name:"Sangam Bartaula", role:"Data Scientist", company:"Data Insights Co.", class:2021, location:"Austin", linkedin:"https://www.linkedin.com/in/sangambartaula/"},
  { id:3, name:"Shrish Acharya", role:"Product Manager", company:"Innovate Labs", class:2023, location:"Houston", linkedin:"https://www.linkedin.com/in/shrish-acharya-53b46932b/"},
  { id:4, name:"Niranjan Paudel", role:"Cybersecurity Analyst", company:"SecureNet Systems", class:2020, location:"Dallas", linkedin:"https://www.linkedin.com/in/niranjan-paudel-14a31a330/"},
  { id:5, name:"Abishek Lamichhane", role:"Cloud Architect", company:"Global Cloud Services", class:2022, location:"Remote", linkedin:"https://www.linkedin.com/in/abishek-lamichhane-b21ab6330/"},
];

// Store user interactions in memory
let userInteractions = {};

// DOM references
const listContainer = document.getElementById('list');
const count = document.getElementById('count');

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
      }
    } else {
      const success = await saveInteraction(p.id, 'bookmarked');
      if (success) {
        item.classList.add('bookmarked');
        starBtn.textContent = '★';
      }
    }
  });
  return item;
}

// Render list to grid
function renderProfiles(list) {
  if (listContainer) {
    listContainer.innerHTML = '';
    list.forEach(p => listContainer.appendChild(createListItem(p)));
  }
  if (count) count.textContent = `(${list.length})`;
}

// Populate filters (location, role, graduation)
function populateFilters(list) {
  const locations = Array.from(new Set(list.map(x => x.location).filter(Boolean))).sort();
  const roles = Array.from(new Set(list.map(x => x.role).filter(Boolean))).sort();
  const years = Array.from(new Set(list.map(x => x.class).filter(Boolean))).sort((a,b)=>b-a);

  const locChecks = document.getElementById('locChecks');
  const roleChecks = document.getElementById('roleChecks');
  const gradSelect = document.getElementById('gradSelect');

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

  if (gradSelect) {
    gradSelect.innerHTML = '<option value=\"\">All years</option>';
    years.forEach(y => {
      const opt = document.createElement('option');
      opt.value = y;
      opt.textContent = y;
      gradSelect.appendChild(opt);
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
      document.querySelectorAll('input[name="location"], input[name="role"]').forEach(cb => cb.checked = false);
      if (gradSelect) gradSelect.value = '';
      if (q) q.value = '';
      apply();
    });
  }

  function getFilters() {
    const term = q ? q.value.trim().toLowerCase() : '';
    const loc = Array.from(document.querySelectorAll('input[name="location"]:checked')).map(i => i.value);
    const role = Array.from(document.querySelectorAll('input[name="role"]:checked')).map(i => i.value);
    const year = gradSelect ? gradSelect.value : '';
    return { term, loc, role, year };
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
      const matchYear = !f.year || String(a.class) === String(f.year);
      return matchTerm && matchLoc && matchRole && matchYear;
    });
    const sortedFiltered = getSorted(filtered);
    renderProfiles(sortedFiltered);
  }

  if (q) q.addEventListener('input', apply);
  document.addEventListener('change', (e) => {
    if (e.target.matches('input[name="location"], input[name="role"], #gradSelect')) apply();
  });
  if (sortSelect) sortSelect.addEventListener('change', apply);
}

// Initialize: fetch alumni from backend and fall back to `fakeAlumni` if necessary
(async function init() {
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
        linkedin: a.linkedin || ''
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

  // Populate UI with the alumni data (from API or fallback)
  populateFilters(alumniData);
  setupFiltering(alumniData);
  // Show all profiles on first load (default sort)
  const sortSelect = document.getElementById('sortSelect');
  if (sortSelect) sortSelect.value = "";
  // Manually trigger initial rendering
  if (typeof renderProfiles === 'function') renderProfiles(alumniData);
})();