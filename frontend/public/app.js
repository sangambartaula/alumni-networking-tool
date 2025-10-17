// app.js
// Fake alumni data (temporary). Remove or replace when backend is ready.
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
const grid = document.getElementById('grid');
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

// Create profile card element
function createCard(p) {
  const card = document.createElement('article');
  card.className = 'card';
  card.setAttribute('data-id', p.id);

  card.innerHTML = `
    <div>
      <h3 class="name">${p.name}</h3>
      <p class="role">${p.role} at ${p.company}</p>
      <div class="class">Class of ${p.class} · ${p.location}</div>
    </div>
    <div class="actions">
      <a class="btn link" href="${p.linkedin}" target="_blank" rel="noopener">LinkedIn Profile
        <svg class="ext" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M18 13v6a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>
          <path d="m15 3 6 6"/>
          <path d="M21 3h-6v6"/>
        </svg>
      </a>
      <button class="btn connect" type="button">Connect</button>
      <button class="btn star" type="button" title="Bookmark this alumni">⭐</button>
    </div>
  `;

  // Connect button action - TOGGLE between Connect and Requested
  const connectBtn = card.querySelector('.btn.connect');
  
  // Check if already connected
  if (hasInteraction(p.id, 'connected')) {
    connectBtn.textContent = 'Requested';
    connectBtn.classList.add('requested');
  }
  
  connectBtn.addEventListener('click', async () => {
    const isCurrentlyConnected = connectBtn.classList.contains('requested');
    
    if (isCurrentlyConnected) {
      // Remove connection
      const success = await removeInteraction(p.id, 'connected');
      if (success) {
        connectBtn.textContent = 'Connect';
        connectBtn.classList.remove('requested');
      }
    } else {
      // Add connection
      const success = await saveInteraction(p.id, 'connected');
      if (success) {
        connectBtn.textContent = 'Requested';
        connectBtn.classList.add('requested');
      }
    }
  });

  // Bookmark button action - TOGGLE between ⭐ and ★
  const starBtn = card.querySelector('.btn.star');
  
  // Check if already bookmarked
  if (hasInteraction(p.id, 'bookmarked')) {
    card.classList.add('bookmarked');
    starBtn.textContent = '★';
  }
  
  starBtn.addEventListener('click', async () => {
    const isCurrentlyBookmarked = card.classList.contains('bookmarked');
    
    if (isCurrentlyBookmarked) {
      // Remove bookmark
      const success = await removeInteraction(p.id, 'bookmarked');
      if (success) {
        card.classList.remove('bookmarked');
        starBtn.textContent = '⭐';
      }
    } else {
      // Add bookmark
      const success = await saveInteraction(p.id, 'bookmarked');
      if (success) {
        card.classList.add('bookmarked');
        starBtn.textContent = '★';
      }
    }
  });

  return card;
}

// Render list to grid
function renderProfiles(list) {
  grid.innerHTML = '';
  list.forEach(p => grid.appendChild(createCard(p)));
  if (count) count.textContent = `(${list.length})`;
}

// Populate filters (location, role, graduation)
function populateFilters(list) {
  const locations = Array.from(new Set(list.map(x => x.location))).sort();
  const roles = Array.from(new Set(list.map(x => x.role))).sort();
  const years = Array.from(new Set(list.map(x => x.class))).sort((a,b)=>b-a);

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

  function getFilters() {
    const term = q ? q.value.trim().toLowerCase() : '';
    const loc = Array.from(document.querySelectorAll('input[name="location"]:checked')).map(i => i.value);
    const role = Array.from(document.querySelectorAll('input[name="role"]:checked')).map(i => i.value);
    const year = gradSelect ? gradSelect.value : '';
    return { term, loc, role, year };
  }

  function apply() {
    const f = getFilters();
    const out = list.filter(a => {
      const t = `${a.name} ${a.role} ${a.company}`.toLowerCase();
      const matchTerm = !f.term || t.includes(f.term);
      const matchLoc = !f.loc.length || f.loc.includes(a.location);
      const matchRole = !f.role.length || f.role.includes(a.role);
      const matchYear = !f.year || String(a.class) === String(f.year);
      return matchTerm && matchLoc && matchRole && matchYear;
    });
    renderProfiles(out);
  }

  if (q) q.addEventListener('input', apply);
  document.addEventListener('change', (e) => {
    if (e.target.matches('input[name="location"], input[name="role"], #gradSelect')) apply();
  });
}

// Initialize
(async function init() {
  // Load interactions from backend first
  await loadUserInteractions();
  
  populateFilters(fakeAlumni);
  renderProfiles(fakeAlumni);
  setupFiltering(fakeAlumni);
  
  // Sorting feature
  const sortSelect = document.getElementById("sortSelect");
  sortSelect?.addEventListener("change", () => {
    const value = sortSelect.value;
    let sorted = [...fakeAlumni];

    if (value === "name") {
      sorted.sort((a, b) => a.name.localeCompare(b.name));
    } else if (value === "year") {
      sorted.sort((a, b) => b.class - a.class);
    }

    renderProfiles(sorted);
  });
})();