// events.js — Events tab: simplified heatmap + location-based filtering

// ═══════════════════════════════════════════════
// SHARED STATE (exposed for future integration)
// ═══════════════════════════════════════════════
window.eventsState = {
  selectedLocation: null,      // { name, lat, lng }
  selectedLocationType: null,  // 'city' | 'state' | 'country' | 'pin'
  radiusValue: 50,             // miles (only relevant for city/pin)
  filteredUsers: [],
  visitedLinkedInIds: new Set(),
};

// ═══════════════════════════════════════════════
// INTERNAL STATE
// ═══════════════════════════════════════════════
let eventsMap = null;
let eventsHeatLayer = null;
let eventsMarkers = [];
let eventsPinMarker = null;
let eventsLocationClusters = [];
let eventsCurrentPage = 1;
const EVENTS_PAGE_SIZE = 25;
const EVENTS_BUILDER_STORAGE_KEY = 'events_builder_state_v1';
const DEFAULT_EVENT_TEMPLATES = [
  {
    id: 'formal-dean-invite',
    name: 'Formal Dean Invite',
    body: `Dear {first_name},
On behalf of {sender_org}, {sender_name} would like to formally invite you to {event_name}.
This event will take place at {location} on {date} at {time}.
Additional Info: {additional_info}
We would be honored by your presence. Please RSVP here: {rsvp_link}`,
  },
  {
    id: 'alumni-community-invite',
    name: 'Alumni Community Invite',
    body: `Hi {first_name},
{sender_name} from {sender_org} is excited to invite you to {event_name}.
This event will take place at {location} on {date} at {time}.
Additional Info: {additional_info}
Hope you can join us, RSVP here: {rsvp_link}`,
  },
];
let eventsToastTimer = null;

/*
Internal integration summary:
- Shared source of truth: window.eventsState.selectedLocation, selectedLocationType, radiusValue, filteredUsers.
- Filtered users are produced in applyLocationFilter() from /api/heatmap sample_alumni payloads, then rendered by createEventsListItem().
- Card data shape includes id, name, first, last, role, company, location, class, linkedin, linkedin_url, degree, _distance, _lat, _lng.
- LinkedIn URLs are accessed through p.linkedin / p.linkedin_url.
- Prior card actions on Events were profile modal open plus direct LinkedIn anchor. Events-only behavior below replaces that with a single View LinkedIn action.
*/

// Aggregated lookup tables (built from heatmap data)
let cityIndex = {};      // { "dallas, texas, united states": { count, lat, lng, state, country } }
let stateIndex = {};     // { "texas": { count, cities: [...] } }
let countryIndex = {};   // { "united states": { count, states: [...] } }

// US state abbreviation map
const US_STATE_ABBR = {
  'al': 'alabama', 'ak': 'alaska', 'az': 'arizona', 'ar': 'arkansas',
  'ca': 'california', 'co': 'colorado', 'ct': 'connecticut', 'de': 'delaware',
  'fl': 'florida', 'ga': 'georgia', 'hi': 'hawaii', 'id': 'idaho',
  'il': 'illinois', 'in': 'indiana', 'ia': 'iowa', 'ks': 'kansas',
  'ky': 'kentucky', 'la': 'louisiana', 'me': 'maine', 'md': 'maryland',
  'ma': 'massachusetts', 'mi': 'michigan', 'mn': 'minnesota', 'ms': 'mississippi',
  'mo': 'missouri', 'mt': 'montana', 'ne': 'nebraska', 'nv': 'nevada',
  'nh': 'new hampshire', 'nj': 'new jersey', 'nm': 'new mexico', 'ny': 'new york',
  'nc': 'north carolina', 'nd': 'north dakota', 'oh': 'ohio', 'ok': 'oklahoma',
  'or': 'oregon', 'pa': 'pennsylvania', 'ri': 'rhode island', 'sc': 'south carolina',
  'sd': 'south dakota', 'tn': 'tennessee', 'tx': 'texas', 'ut': 'utah',
  'vt': 'vermont', 'va': 'virginia', 'wa': 'washington', 'wv': 'west virginia',
  'wi': 'wisconsin', 'wy': 'wyoming', 'dc': 'district of columbia',
};

// ═══════════════════════════════════════════════
// UTILITY FUNCTIONS
// ═══════════════════════════════════════════════
function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function escapeAttribute(value) {
  return String(value).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function highlightMatch(text, query) {
  if (!query) return escapeHtml(text);
  const escaped = query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`(${escaped})`, 'gi');
  return escapeHtml(text).replace(regex, '<strong>$1</strong>');
}

/** Haversine distance in miles */
function haversineDistance(lat1, lon1, lat2, lon2) {
  const R = 3958.8; // Earth radius in miles
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

/** Parse "City, State, Country" location string into parts */
function parseLocationParts(locationStr) {
  if (!locationStr) return { city: '', state: '', country: '' };
  const parts = locationStr.split(',').map(s => s.trim());
  if (parts.length >= 3) {
    return { city: parts[0], state: parts[1], country: parts.slice(2).join(', ') };
  }
  if (parts.length === 2) {
    return { city: parts[0], state: parts[1], country: '' };
  }
  return { city: parts[0], state: '', country: '' };
}

/** Resolve a state abbreviation to its full name */
function resolveStateAbbr(input) {
  const lower = (input || '').trim().toLowerCase();
  return US_STATE_ABBR[lower] || lower;
}

function getDefaultBuilderState() {
  return {
    event_name: '',
    event_date: '',
    event_time: '',
    event_location: '',
    sender_name: '',
    sender_org: '',
    rsvp_link: '',
    additional_info: '',
    use_default_templates: true,
    custom_template_body: `Dear {first_name}, {sender_name} from {sender_org} would like to invite you to {event_name}. This event will take place at {location} on {date} at {time}. Additional info: {additional_info}. Please RSVP here: {rsvp_link}`,
  };
}

function loadBuilderState() {
  const defaults = getDefaultBuilderState();
  try {
    const raw = localStorage.getItem(EVENTS_BUILDER_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    return {
      ...defaults,
      ...parsed,
    };
  } catch (_error) {
    return defaults;
  }
}

function persistBuilderState() {
  const payload = {
    event_name: document.getElementById('eventNameInput')?.value.trim() || '',
    event_date: document.getElementById('eventDateInput')?.value || '',
    event_time: document.getElementById('eventTimeInput')?.value || '',
    event_location: document.getElementById('eventLocationInput')?.value.trim() || '',
    sender_name: document.getElementById('senderNameInput')?.value.trim() || '',
    sender_org: document.getElementById('senderOrgInput')?.value.trim() || '',
    rsvp_link: document.getElementById('rsvpLinkInput')?.value.trim() || '',
    additional_info: document.getElementById('additionalInfoInput')?.value.trim() || '',
    use_default_templates: Boolean(document.getElementById('useDefaultTemplatesInput')?.checked),
    custom_template_body: document.getElementById('customTemplateBodyInput')?.value || '',
  };
  localStorage.setItem(EVENTS_BUILDER_STORAGE_KEY, JSON.stringify(payload));
}

function getTemplateForMode(isDefaultMode) {
  if (isDefaultMode) {
    const index = Math.floor(Math.random() * DEFAULT_EVENT_TEMPLATES.length);
    return DEFAULT_EVENT_TEMPLATES[index] || DEFAULT_EVENT_TEMPLATES[0];
  }
  const customBody = document.getElementById('customTemplateBodyInput')?.value || '';
  return {
    id: 'custom-template',
    name: 'Custom Template',
    body: customBody || getDefaultBuilderState().custom_template_body,
  };
}

function syncTemplateModeUI() {
  const useDefault = Boolean(document.getElementById('useDefaultTemplatesInput')?.checked);
  const customBodyInput = document.getElementById('customTemplateBodyInput');
  if (!customBodyInput) return;
  customBodyInput.disabled = useDefault;
  customBodyInput.title = useDefault
    ? 'Turn off Use Default Templates to edit your custom message body.'
    : '';
}

function formatDisplayDate(dateValue) {
  if (!dateValue) return '';
  const parsed = new Date(`${dateValue}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return dateValue;
  return parsed.toLocaleDateString(undefined, { month: 'long', day: 'numeric', year: 'numeric' });
}

function formatDisplayTime(timeValue) {
  if (!timeValue) return '';
  const [hourText, minuteText] = String(timeValue).split(':');
  const hour = Number(hourText);
  const minute = Number(minuteText || 0);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return timeValue;
  const date = new Date();
  date.setHours(hour, minute, 0, 0);
  return date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' });
}

function getBuilderValues() {
  const locationName = document.getElementById('eventLocationInput')?.value.trim() || '';
  return {
    event_name: document.getElementById('eventNameInput')?.value.trim() || '',
    event_date: document.getElementById('eventDateInput')?.value || '',
    event_time: document.getElementById('eventTimeInput')?.value || '',
    event_location: locationName,
    sender_name: document.getElementById('senderNameInput')?.value.trim() || '',
    sender_org: document.getElementById('senderOrgInput')?.value.trim() || '',
    rsvp_link: document.getElementById('rsvpLinkInput')?.value.trim() || '',
    additional_info: document.getElementById('additionalInfoInput')?.value.trim() || '',
    location: locationName,
    date: formatDisplayDate(document.getElementById('eventDateInput')?.value || ''),
    time: formatDisplayTime(document.getElementById('eventTimeInput')?.value || ''),
    use_default_templates: Boolean(document.getElementById('useDefaultTemplatesInput')?.checked),
  };
}

function chooseTemplateForClick() {
  const values = getBuilderValues();
  return getTemplateForMode(values.use_default_templates);
}

function cleanupGeneratedMessage(message) {
  return message
    .replace(/\{[a-z0-9_]+\}/gi, '')
    .split('\n')
    .map(line => line.trim())
    .filter(line => {
      const trimmed = line.trim();
      if (!trimmed) return false;
      if (/^[A-Za-z ]+:\s*$/.test(trimmed)) return false;
      return true;
    })
    .join(' ')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function generateMessageForUser(user, templateOverride) {
  const template = templateOverride || chooseTemplateForClick();
  const builderValues = getBuilderValues();
  const nameParts = String(user.name || '').trim().split(/\s+/).filter(Boolean);
  const placeholders = {
    first_name: user.first || nameParts[0] || '',
    last_name: user.last || nameParts.slice(1).join(' ') || '',
    full_name: user.name || '',
    company: user.company || '',
    role: user.role || '',
    location: builderValues.location,
    event_name: builderValues.event_name,
    event_date: builderValues.event_date,
    event_time: builderValues.event_time,
    sender_name: builderValues.sender_name,
    sender_org: builderValues.sender_org,
    rsvp_link: builderValues.rsvp_link,
    additional_info: builderValues.additional_info,
    date: builderValues.date,
    time: builderValues.time,
  };

  const rendered = (template?.body || '').replace(/\{([a-z0-9_]+)\}/gi, (_match, key) => placeholders[key] || '');
  return {
    message: cleanupGeneratedMessage(rendered),
    template,
  };
}

async function copyTextToClipboard(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.setAttribute('readonly', '');
  textarea.style.position = 'absolute';
  textarea.style.left = '-9999px';
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand('copy');
  document.body.removeChild(textarea);
}

function showEventsToast(message, isError = false) {
  const toast = document.getElementById('eventsToast');
  if (!toast) return;
  toast.textContent = message;
  toast.style.color = isError ? '#b42318' : '#166534';
  window.clearTimeout(eventsToastTimer);
  eventsToastTimer = window.setTimeout(() => {
    toast.textContent = '';
  }, 3200);
}

function markVisitedLinkedIn(userId) {
  window.eventsState.visitedLinkedInIds.add(userId);
}

async function handleLinkedInOutreach(user) {
  const linkedInUrl = user.linkedin || user.linkedin_url;
  if (!linkedInUrl) {
    showEventsToast('This alumni record does not have a LinkedIn URL.', true);
    return;
  }

  const { message, template } = generateMessageForUser(user);
  persistBuilderState();

  try {
    await copyTextToClipboard(message);
    markVisitedLinkedIn(user.id);
    window.open(linkedInUrl, '_blank', 'noopener');
    showEventsToast(`Copied outreach message using “${template.name}” and opened LinkedIn.`);
    renderCurrentPage();
  } catch (error) {
    console.error('Failed to copy outreach message:', error);
    showEventsToast('Unable to copy the message to clipboard. Please try again.', true);
  }
}

function initializeEventBuilder() {
  const savedState = loadBuilderState();

  const fieldMap = {
    eventNameInput: savedState.event_name,
    eventDateInput: savedState.event_date,
    eventTimeInput: savedState.event_time,
    eventLocationInput: savedState.event_location,
    senderNameInput: savedState.sender_name,
    senderOrgInput: savedState.sender_org,
    rsvpLinkInput: savedState.rsvp_link,
    additionalInfoInput: savedState.additional_info,
    customTemplateBodyInput: savedState.custom_template_body,
  };

  Object.entries(fieldMap).forEach(([id, value]) => {
    const element = document.getElementById(id);
    if (element) element.value = value || '';
  });

  const defaultToggle = document.getElementById('useDefaultTemplatesInput');
  if (defaultToggle) defaultToggle.checked = Boolean(savedState.use_default_templates);
  syncTemplateModeUI();

  console.info('Events integration summary', {
    stateKeys: Object.keys(window.eventsState),
    userShape: ['id', 'name', 'first', 'last', 'role', 'company', 'location', 'class', 'linkedin', 'linkedin_url', '_distance'],
    linkedInFields: ['linkedin', 'linkedin_url'],
    cardAction: 'View LinkedIn copies template message and opens LinkedIn.',
  });
}

// ═══════════════════════════════════════════════
// EDUCATION / DISPLAY HELPERS (copied from app.js)
// ═══════════════════════════════════════════════
function _isMeaningfulEducationValue(value) {
  if (!value) return false;
  const trimmed = String(value).trim();
  if (!trimmed) return false;
  const lowered = trimmed.toLowerCase();
  if (['unknown', 'not found', 'n/a', 'na', 'none', 'null', 'nan', 'other'].includes(lowered)) return false;
  if (lowered === 'university of north texas' || lowered === 'unt') return false;
  return true;
}

function _sanitizeEducationSnippet(value) {
  if (!value) return '';
  let text = String(value).trim();
  if (!text) return '';
  text = text.replace(
    /^\s*(?:[A-Za-z]{3,9}\s+)?(?:19|20)\d{2}\s*[-–—]\s*(?:[A-Za-z]{3,9}\s+)?(?:(?:19|20)\d{2}|Present)\s*[-–—:]\s*/i,
    ''
  );
  text = text.replace(/^\s*Class\s+of\s+(?:19|20)\d{2}\s*[-–—:]\s*/i, '');
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
    ? _sanitizeEducationSnippet(profile.full_degree) : '';
  const fullMajor = _isMeaningfulEducationValue(profile.full_major)
    ? _sanitizeEducationSnippet(profile.full_major) : '';
  if (!fullDegree && !fullMajor) return '';
  if (fullDegree && fullMajor) {
    if (fullDegree.toLowerCase().includes(fullMajor.toLowerCase())) return fullDegree;
    return `${fullDegree} - ${fullMajor}`;
  }
  return fullDegree || fullMajor;
}

function _buildClassLocationLine(profile) {
  const gradYearRaw = profile.class ? String(profile.class).trim() : '';
  const gradYear = gradYearRaw.replace(/\b(19\d{2}|20\d{2}|2100)\.0\b/g, '$1');
  const location = _isMeaningfulLocationValue(profile.location) ? String(profile.location).trim() : '';
  if (gradYear) return `Class of ${gradYear}${location ? ' - ' + location : ''}`;
  return location;
}

// ═══════════════════════════════════════════════
// CARD RENDERING (copied from app.js createListItem)
// ═══════════════════════════════════════════════
function createEventsListItem(p) {
  const educationLine = _buildEducationLine(p);
  const roleLine = p.role || p.headline || '';
  const classLocationLine = _buildClassLocationLine(p);

  const item = document.createElement('div');
  item.className = 'list-item';
  if (window.eventsState.visitedLinkedInIds.has(p.id)) {
    item.classList.add('is-visited');
  }
  item.setAttribute('data-id', p.id);

  const distanceTag = p._distance != null
    ? `<span style="display:inline-block;margin-left:8px;font-size:0.78rem;color:#667eea;background:rgba(102,126,234,0.08);padding:2px 8px;border-radius:8px;">${Math.round(p._distance)} mi</span>`
    : '';

  item.innerHTML = `
    <div class="list-main">
      <div class="list-details">
        <h3 class="name">${escapeHtml(p.name)}${distanceTag}</h3>
        ${educationLine ? `<p class="education">${escapeHtml(educationLine)}</p>` : ''}
        ${roleLine ? `<p class="role">${escapeHtml(roleLine)}</p>` : ''}
        ${classLocationLine ? `<div class="class">${escapeHtml(classLocationLine)}</div>` : ''}
      </div>
      <div class="list-actions">
        <button class="events-linkedin-btn" type="button" ${p.linkedin ? '' : 'disabled'} title="Copy message and open LinkedIn">
          View LinkedIn
        </button>
      </div>
    </div>
  `;

  const linkedInBtn = item.querySelector('.events-linkedin-btn');
  if (linkedInBtn) {
    linkedInBtn.addEventListener('click', () => {
      handleLinkedInOutreach(p);
    });
  }

  return item;
}

// ═══════════════════════════════════════════════
// MAP INITIALIZATION (Leaflet 2D only — simplified)
// ═══════════════════════════════════════════════
function initEventsMap() {
  eventsMap = L.map('eventsMapContainer', {
    center: [32.75, -97.33], // Default: DFW area
    zoom: 4,
    zoomControl: true,
  });

  L.tileLayer('https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
    attribution: 'Google Earth',
    maxZoom: 20,
  }).addTo(eventsMap);

  // Click to drop pin
  eventsMap.on('click', (e) => {
    dropPin(e.latlng.lat, e.latlng.lng);
  });
}

function addCircleMarker(location) {
  const count = location.count || 1;
  const color = count >= 10 ? '#FF4444' : count >= 7 ? '#FF8C00' : count >= 5 ? '#FFD700' : count >= 3 ? '#32CD32' : '#00CED1';

  const marker = L.circleMarker([location.latitude, location.longitude], {
    radius: 8 + Math.sqrt(count) * 2,
    fillColor: color,
    color: '#fff',
    weight: 2,
    opacity: 1,
    fillOpacity: 0.8,
  });

  const popupContent = `
    <div style="font-family: Inter, sans-serif; max-width: 250px;">
      <h4 style="margin:0 0 4px; color:#667eea;">${escapeHtml(location.location)}</h4>
      <div style="color:#475569; font-size:13px;">${count} alumni</div>
    </div>
  `;
  marker.bindPopup(popupContent, { maxWidth: 280 });
  marker.addTo(eventsMap);
  eventsMarkers.push(marker);
}

function renderHeatLayer(locations) {
  if (eventsHeatLayer) {
    eventsMap.removeLayer(eventsHeatLayer);
    eventsHeatLayer = null;
  }
  if (!locations || locations.length === 0) return;

  const maxCount = Math.max(...locations.map(l => l.count || 1));
  const heatPoints = locations.map(l => [
    l.latitude,
    l.longitude,
    (l.count || 1) / (maxCount || 1),
  ]);

  eventsHeatLayer = L.heatLayer(heatPoints, {
    radius: 45,
    blur: 30,
    maxZoom: 8,
    gradient: {
      0.2: '#00bfff',
      0.4: '#00ff7f',
      0.6: '#ffff00',
      0.8: '#ff8c00',
      1.0: '#ff0000',
    },
  }).addTo(eventsMap);
}

// ═══════════════════════════════════════════════
// DATA LOADING — Fetch from existing /api/heatmap
// ═══════════════════════════════════════════════
async function loadEventsData() {
  try {
    const response = await fetch('/api/heatmap?_t=' + Date.now());
    const data = await response.json();

    if (!data.success) {
      console.error('Failed to load heatmap data');
      return;
    }

    eventsLocationClusters = data.locations || [];

    // Clear old markers
    eventsMarkers.forEach(m => eventsMap.removeLayer(m));
    eventsMarkers = [];

    // Add markers + heat layer
    eventsLocationClusters.forEach(loc => addCircleMarker(loc));
    renderHeatLayer(eventsLocationClusters);

    // Build aggregation indexes
    buildLocationIndexes();
  } catch (error) {
    console.error('Error loading events data:', error);
  }
}

// ═══════════════════════════════════════════════
// BUILD AGGREGATION INDEXES
// ═══════════════════════════════════════════════
function buildLocationIndexes() {
  cityIndex = {};
  stateIndex = {};
  countryIndex = {};

  eventsLocationClusters.forEach(cluster => {
    const alumni = cluster.sample_alumni || [];
    const locStr = (cluster.location || '').trim();
    if (!locStr) return;

    const parts = parseLocationParts(locStr);
    const cityKey = locStr.toLowerCase();
    const stateKey = (parts.state || '').toLowerCase();
    const countryKey = (parts.country || '').toLowerCase();

    // City index
    if (!cityIndex[cityKey]) {
      cityIndex[cityKey] = {
        name: locStr,
        lat: cluster.latitude,
        lng: cluster.longitude,
        state: parts.state,
        country: parts.country,
        count: 0,
        alumni: [],
      };
    }
    cityIndex[cityKey].count += alumni.length;
    cityIndex[cityKey].alumni.push(...alumni.map(a => ({
      ...a,
      _lat: cluster.latitude,
      _lng: cluster.longitude,
      location: locStr,
    })));

    // State index
    if (stateKey) {
      if (!stateIndex[stateKey]) {
        stateIndex[stateKey] = { name: parts.state, count: 0, cities: new Set() };
      }
      stateIndex[stateKey].count += alumni.length;
      stateIndex[stateKey].cities.add(cityKey);
    }

    // Country index
    if (countryKey) {
      if (!countryIndex[countryKey]) {
        countryIndex[countryKey] = { name: parts.country, count: 0, states: new Set() };
      }
      countryIndex[countryKey].count += alumni.length;
      if (stateKey) countryIndex[countryKey].states.add(stateKey);
    }
  });
}

// ═══════════════════════════════════════════════
// AUTOCOMPLETE (adopted from heatmap_dual.js pattern)
// ═══════════════════════════════════════════════
function buildAutocompleteSuggestions(query) {
  const results = [];
  const lowerQ = query.toLowerCase();

  // 1. City matches
  for (const [key, data] of Object.entries(cityIndex)) {
    if (key.includes(lowerQ)) {
      results.push({ type: 'city', name: data.name, count: data.count, lat: data.lat, lng: data.lng });
    }
  }

  // 2. State matches (including abbreviations)
  const resolvedQ = resolveStateAbbr(query);
  for (const [key, data] of Object.entries(stateIndex)) {
    if (key.includes(lowerQ) || key.includes(resolvedQ)) {
      results.push({ type: 'state', name: data.name, count: data.count });
    }
  }

  // 3. Country matches
  for (const [key, data] of Object.entries(countryIndex)) {
    if (key.includes(lowerQ)) {
      results.push({ type: 'country', name: data.name, count: data.count });
    }
  }

  // Sort by count descending, then alphabetically
  results.sort((a, b) => {
    if (b.count !== a.count) return b.count - a.count;
    return a.name.localeCompare(b.name);
  });

  // Dedupe by name+type
  const seen = new Set();
  return results.filter(r => {
    const key = `${r.type}:${r.name.toLowerCase()}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).slice(0, 15);
}

function renderSuggestions(suggestions, query) {
  const dropdown = document.getElementById('eventsSuggestionsDropdown');
  if (!dropdown) return;

  if (!suggestions || suggestions.length === 0) {
    if (query && query.length >= 1) {
      dropdown.innerHTML = '<div class="events-suggestion-no-results">No matching locations found</div>';
      dropdown.style.display = 'block';
    } else {
      dropdown.style.display = 'none';
    }
    return;
  }

  dropdown.innerHTML = suggestions.map(s => `
    <div class="events-suggestion-item"
         data-type="${s.type}"
         data-name="${escapeAttribute(s.name)}"
         ${s.lat != null ? `data-lat="${s.lat}" data-lng="${s.lng}"` : ''}>
      <span class="events-suggestion-text">${highlightMatch(s.name, query)}</span>
      <span class="events-suggestion-type ${s.type}">${s.type}</span>
      <span class="events-suggestion-count">${s.count}</span>
    </div>
  `).join('');

  dropdown.style.display = 'block';
}

function showDefaultSuggestions() {
  // Show top locations by user count when the input is empty/focused
  const all = [];

  for (const data of Object.values(cityIndex)) {
    all.push({ type: 'city', name: data.name, count: data.count, lat: data.lat, lng: data.lng });
  }
  for (const data of Object.values(stateIndex)) {
    all.push({ type: 'state', name: data.name, count: data.count });
  }

  all.sort((a, b) => b.count - a.count);

  const dropdown = document.getElementById('eventsSuggestionsDropdown');
  if (!dropdown) return;

  const top = all.slice(0, 10);
  if (top.length === 0) {
    dropdown.style.display = 'none';
    return;
  }

  dropdown.innerHTML =
    '<div class="events-suggestion-header">Popular Locations</div>' +
    top.map(s => `
      <div class="events-suggestion-item"
           data-type="${s.type}"
           data-name="${escapeAttribute(s.name)}"
           ${s.lat != null ? `data-lat="${s.lat}" data-lng="${s.lng}"` : ''}>
        <span class="events-suggestion-text">${escapeHtml(s.name)}</span>
        <span class="events-suggestion-type ${s.type}">${s.type}</span>
        <span class="events-suggestion-count">${s.count}</span>
      </div>
    `).join('');

  dropdown.style.display = 'block';
}

// ═══════════════════════════════════════════════
// LOCATION SELECTION
// ═══════════════════════════════════════════════
function selectLocation(type, name, lat, lng) {
  const state = window.eventsState;
  state.selectedLocationType = type;
  state.selectedLocation = { name, lat: lat || null, lng: lng || null };

  const controlsBar = document.getElementById('eventsControlsBar');
  const badgeType = document.getElementById('eventsBadgeType');
  const badgeName = document.getElementById('eventsBadgeName');
  const radiusControl = document.getElementById('eventsRadiusControl');

  controlsBar.style.display = 'flex';
  badgeType.textContent = type.toUpperCase();
  badgeType.className = 'badge-type ' + type;
  badgeName.textContent = name;

  // Show radius only for city/pin
  if (type === 'city' || type === 'pin') {
    radiusControl.style.display = 'flex';
  } else {
    radiusControl.style.display = 'none';
  }

  // Zoom map to location
  if (lat != null && lng != null) {
    const zoomLevel = type === 'pin' ? 10 : type === 'city' ? 8 : type === 'state' ? 6 : 4;
    eventsMap.setView([lat, lng], zoomLevel);
  }

  // Apply filter
  applyLocationFilter();
}

function dropPin(lat, lng) {
  // Remove existing pin
  if (eventsPinMarker) {
    eventsMap.removeLayer(eventsPinMarker);
  }

  // Add new pin marker
  eventsPinMarker = L.marker([lat, lng], {
    title: 'Selected Location',
  }).addTo(eventsMap);

  eventsPinMarker.bindPopup(`
    <div style="font-family: Inter, sans-serif;">
      <strong style="color:#ef4444;">📍 Dropped Pin</strong><br/>
      <span style="color:#64748b; font-size:12px;">${lat.toFixed(4)}, ${lng.toFixed(4)}</span>
    </div>
  `).openPopup();

  const input = document.getElementById('eventsSearchInput');
  if (input) input.value = `${lat.toFixed(4)}, ${lng.toFixed(4)}`;

  selectLocation('pin', `${lat.toFixed(4)}, ${lng.toFixed(4)}`, lat, lng);
}

function clearSelection() {
  const state = window.eventsState;
  state.selectedLocation = null;
  state.selectedLocationType = null;
  state.filteredUsers = [];

  // Hide controls bar
  document.getElementById('eventsControlsBar').style.display = 'none';

  // Clear search input
  const input = document.getElementById('eventsSearchInput');
  if (input) input.value = '';

  // Remove pin
  if (eventsPinMarker) {
    eventsMap.removeLayer(eventsPinMarker);
    eventsPinMarker = null;
  }

  // Reset view
  eventsMap.setView([32.75, -97.33], 4);

  // Show empty state
  renderEmptyState();
  updateResultsCount(0);

  // Clear pagination
  const pagination = document.getElementById('eventsPagination');
  if (pagination) pagination.innerHTML = '';

  eventsCurrentPage = 1;
}

// ═══════════════════════════════════════════════
// FILTERING LOGIC
// ═══════════════════════════════════════════════
function applyLocationFilter() {
  const state = window.eventsState;
  const { selectedLocation, selectedLocationType } = state;
  if (!selectedLocation || !selectedLocationType) return;

  const radiusInput = document.getElementById('eventsRadiusInput');
  const radius = parseFloat(radiusInput?.value) || 50;
  state.radiusValue = radius;

  let filteredAlumni = [];

  switch (selectedLocationType) {
    case 'city': {
      const { lat, lng } = selectedLocation;
      if (lat != null && lng != null && radius > 0) {
        // Radius-based filtering using Haversine
        for (const cluster of eventsLocationClusters) {
          const dist = haversineDistance(lat, lng, cluster.latitude, cluster.longitude);
          if (dist <= radius) {
            (cluster.sample_alumni || []).forEach(a => {
              filteredAlumni.push(mapHeatmapAlumniToCard(a, cluster, dist));
            });
          }
        }
      } else {
        // Exact city match (radius = 0)
        const cityKey = selectedLocation.name.toLowerCase();
        const cityData = cityIndex[cityKey];
        if (cityData) {
          filteredAlumni = cityData.alumni.map(a => mapHeatmapAlumniToCard(a, null, 0));
        }
      }
      break;
    }

    case 'pin': {
      const { lat, lng } = selectedLocation;
      if (lat != null && lng != null) {
        for (const cluster of eventsLocationClusters) {
          const dist = haversineDistance(lat, lng, cluster.latitude, cluster.longitude);
          if (dist <= radius) {
            (cluster.sample_alumni || []).forEach(a => {
              filteredAlumni.push(mapHeatmapAlumniToCard(a, cluster, dist));
            });
          }
        }
      }
      break;
    }

    case 'state': {
      const stateKey = resolveStateAbbr(selectedLocation.name);
      const stateData = stateIndex[stateKey];
      if (stateData) {
        for (const cityKey of stateData.cities) {
          const city = cityIndex[cityKey];
          if (city) {
            city.alumni.forEach(a => {
              filteredAlumni.push(mapHeatmapAlumniToCard(a, null, null));
            });
          }
        }
      }
      break;
    }

    case 'country': {
      const countryKey = selectedLocation.name.toLowerCase();
      const countryData = countryIndex[countryKey];
      if (countryData) {
        for (const stKey of countryData.states) {
          const stData = stateIndex[stKey];
          if (stData) {
            for (const cityKey of stData.cities) {
              const city = cityIndex[cityKey];
              if (city) {
                city.alumni.forEach(a => {
                  filteredAlumni.push(mapHeatmapAlumniToCard(a, null, null));
                });
              }
            }
          }
        }
        // Also include cities that have no state but match the country
        for (const [cityKey, city] of Object.entries(cityIndex)) {
          if ((city.country || '').toLowerCase() === countryKey && !(city.state || '').trim()) {
            city.alumni.forEach(a => {
              filteredAlumni.push(mapHeatmapAlumniToCard(a, null, null));
            });
          }
        }
      }
      break;
    }
  }

  // Deduplicate by alumni ID
  const seen = new Set();
  filteredAlumni = filteredAlumni.filter(a => {
    if (seen.has(a.id)) return false;
    seen.add(a.id);
    return true;
  });

  // Sort
  applySortOrder(filteredAlumni);

  state.filteredUsers = filteredAlumni;
  eventsCurrentPage = 1;

  updateResultsCount(filteredAlumni.length);
  renderCurrentPage();
}

function mapHeatmapAlumniToCard(alumni, cluster, distance) {
  const name = alumni.name || '';
  const parts = name.split(/\s+/);
  const clusterLocation = (cluster?.location || '').trim();
  const alumniLocation = (alumni.location || '').trim();
  const resolvedLocation = _isMeaningfulLocationValue(alumniLocation) ? alumniLocation : clusterLocation;

  return {
    id: alumni.id,
    name: name,
    first: parts[0] || '',
    last: parts.length > 1 ? parts.slice(1).join(' ') : '',
    role: alumni.role || alumni.position || '',
    headline: '',
    company: alumni.company || '',
    // Prefer each profile's own location label; fallback to cluster label.
    location: resolvedLocation,
    class: alumni.grad_year || '',
    linkedin: alumni.linkedin || '',
    linkedin_url: alumni.linkedin || '',
    degree: alumni.degree || '',
    full_degree: '',
    full_major: '',
    _distance: distance,
    _lat: alumni._lat || (cluster ? cluster.latitude : null),
    _lng: alumni._lng || (cluster ? cluster.longitude : null),
  };
}

function applySortOrder(alumni) {
  const sortSelect = document.getElementById('eventsSortSelect');
  const sortBy = sortSelect ? sortSelect.value : 'name';

  alumni.sort((a, b) => {
    switch (sortBy) {
      case 'distance':
        return (a._distance ?? Infinity) - (b._distance ?? Infinity);
      case 'location':
        return (a.location || '').localeCompare(b.location || '');
      case 'name':
      default:
        return (a.name || '').localeCompare(b.name || '');
    }
  });
}

// ═══════════════════════════════════════════════
// RENDERING
// ═══════════════════════════════════════════════
function updateResultsCount(count) {
  const el = document.getElementById('eventsResultsCount');
  if (el) el.textContent = count;
}

function renderEmptyState() {
  const container = document.getElementById('eventsListContainer');
  const emptyState = document.getElementById('eventsEmptyState');
  if (container) container.innerHTML = '';
  if (emptyState) {
    container.appendChild(emptyState);
    emptyState.style.display = 'flex';
  }
  const pagination = document.getElementById('eventsPagination');
  if (pagination) pagination.innerHTML = '';
}

function renderCurrentPage() {
  const state = window.eventsState;
  const alumni = state.filteredUsers;
  const container = document.getElementById('eventsListContainer');
  const emptyState = document.getElementById('eventsEmptyState');

  if (!container) return;
  container.innerHTML = '';

  if (!alumni || alumni.length === 0) {
    if (state.selectedLocation) {
      container.innerHTML = `
        <div class="events-empty-state">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="width:56px;height:56px;color:#cbd5e1;margin-bottom:16px;">
            <circle cx="11" cy="11" r="8"></circle>
            <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
          </svg>
          <h3>No Alumni Found</h3>
          <p>Try expanding the radius or selecting a different location.</p>
        </div>
      `;
    } else {
      renderEmptyState();
    }
    const pagination = document.getElementById('eventsPagination');
    if (pagination) pagination.innerHTML = '';
    return;
  }

  const totalPages = Math.ceil(alumni.length / EVENTS_PAGE_SIZE);
  if (eventsCurrentPage > totalPages) eventsCurrentPage = totalPages;
  if (eventsCurrentPage < 1) eventsCurrentPage = 1;

  const start = (eventsCurrentPage - 1) * EVENTS_PAGE_SIZE;
  const pageAlumni = alumni.slice(start, start + EVENTS_PAGE_SIZE);

  pageAlumni.forEach(p => container.appendChild(createEventsListItem(p)));

  renderEventsPagination(alumni.length, totalPages);
}

function renderEventsPagination(total, totalPages) {
  const container = document.getElementById('eventsPagination');
  if (!container) return;
  container.innerHTML = '';

  if (totalPages <= 1) return;

  const start = (eventsCurrentPage - 1) * EVENTS_PAGE_SIZE + 1;
  const end = Math.min(eventsCurrentPage * EVENTS_PAGE_SIZE, total);

  const status = document.createElement('span');
  status.className = 'pagination-ellipsis';
  status.textContent = `Showing ${start}-${end} of ${total}`;
  container.appendChild(status);

  // Prev
  const prev = document.createElement('button');
  prev.type = 'button';
  prev.className = 'pagination-btn';
  prev.textContent = 'Prev';
  prev.disabled = eventsCurrentPage <= 1;
  prev.addEventListener('click', () => { eventsCurrentPage--; renderCurrentPage(); });
  container.appendChild(prev);

  // Page numbers
  const maxVisible = 5;
  let pageStart = Math.max(1, eventsCurrentPage - Math.floor(maxVisible / 2));
  let pageEnd = Math.min(totalPages, pageStart + maxVisible - 1);
  if ((pageEnd - pageStart + 1) < maxVisible) {
    pageStart = Math.max(1, pageEnd - maxVisible + 1);
  }

  for (let p = pageStart; p <= pageEnd; p++) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = p === eventsCurrentPage ? 'pagination-btn active' : 'pagination-btn';
    btn.textContent = String(p);
    if (p !== eventsCurrentPage) {
      btn.addEventListener('click', () => { eventsCurrentPage = p; renderCurrentPage(); });
    }
    container.appendChild(btn);
  }

  // Next
  const next = document.createElement('button');
  next.type = 'button';
  next.className = 'pagination-btn';
  next.textContent = 'Next';
  next.disabled = eventsCurrentPage >= totalPages;
  next.addEventListener('click', () => { eventsCurrentPage++; renderCurrentPage(); });
  container.appendChild(next);
}

// ═══════════════════════════════════════════════
// EVENT LISTENERS
// ═══════════════════════════════════════════════
function setupEventListeners() {
  const searchInput = document.getElementById('eventsSearchInput');
  const dropdown = document.getElementById('eventsSuggestionsDropdown');
  const radiusInput = document.getElementById('eventsRadiusInput');
  const clearBtn = document.getElementById('eventsClearBtn');
  const sortSelect = document.getElementById('eventsSortSelect');
  const builderInputs = [
    'eventNameInput',
    'eventDateInput',
    'eventTimeInput',
    'eventLocationInput',
    'senderNameInput',
    'senderOrgInput',
    'rsvpLinkInput',
    'additionalInfoInput',
    'useDefaultTemplatesInput',
    'customTemplateBodyInput',
  ].map(id => document.getElementById(id)).filter(Boolean);

  // Search input → autocomplete
  if (searchInput) {
    searchInput.addEventListener('input', () => {
      const q = searchInput.value.trim();
      if (!q) {
        showDefaultSuggestions();
        return;
      }
      const suggestions = buildAutocompleteSuggestions(q);
      renderSuggestions(suggestions, q);
    });

    searchInput.addEventListener('focus', () => {
      const q = searchInput.value.trim();
      if (!q) {
        showDefaultSuggestions();
      } else {
        const suggestions = buildAutocompleteSuggestions(q);
        renderSuggestions(suggestions, q);
      }
    });

    searchInput.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        dropdown.style.display = 'none';
      }
      if (e.key === 'Enter') {
        e.preventDefault();
        const q = searchInput.value.trim();
        if (!q) return;
        // Try to auto-select best match
        const suggestions = buildAutocompleteSuggestions(q);
        if (suggestions.length > 0) {
          const best = suggestions[0];
          searchInput.value = best.name;
          dropdown.style.display = 'none';
          selectLocation(best.type, best.name, best.lat || null, best.lng || null);

          // For state/country without lat/lng, try to center map on first city
          if (!best.lat && best.type === 'state') {
            centerMapOnState(best.name);
          } else if (!best.lat && best.type === 'country') {
            centerMapOnCountry(best.name);
          }
        }
      }
    });
  }

  // Dropdown click → select suggestion
  if (dropdown) {
    dropdown.addEventListener('mousedown', (e) => e.preventDefault());
    dropdown.addEventListener('click', (e) => {
      const item = e.target.closest('.events-suggestion-item');
      if (!item) return;

      const type = item.dataset.type;
      const name = item.dataset.name;
      const lat = item.dataset.lat ? parseFloat(item.dataset.lat) : null;
      const lng = item.dataset.lng ? parseFloat(item.dataset.lng) : null;

      searchInput.value = name;
      dropdown.style.display = 'none';

      selectLocation(type, name, lat, lng);

      if (!lat && type === 'state') centerMapOnState(name);
      else if (!lat && type === 'country') centerMapOnCountry(name);
    });
  }

  // Click outside → close dropdown
  document.addEventListener('click', (e) => {
    if (dropdown && searchInput && !dropdown.contains(e.target) && e.target !== searchInput) {
      dropdown.style.display = 'none';
    }
  });

  // Radius change → re-filter
  if (radiusInput) {
    let radiusDebounce = null;
    radiusInput.addEventListener('input', () => {
      clearTimeout(radiusDebounce);
      radiusDebounce = setTimeout(() => {
        applyLocationFilter();
      }, 300);
    });
  }

  // Clear button
  if (clearBtn) {
    clearBtn.addEventListener('click', clearSelection);
  }

  // Sort change → re-sort and re-render
  if (sortSelect) {
    sortSelect.addEventListener('change', () => {
      const state = window.eventsState;
      if (state.filteredUsers.length > 0) {
        applySortOrder(state.filteredUsers);
        eventsCurrentPage = 1;
        renderCurrentPage();
      }
    });
  }

  builderInputs.forEach(input => {
    input.addEventListener('input', () => {
      persistBuilderState();
      syncTemplateModeUI();
    });
    input.addEventListener('change', () => {
      persistBuilderState();
      syncTemplateModeUI();
    });
  });
}

// Helper: center map on first city of a state
function centerMapOnState(stateName) {
  const stateKey = resolveStateAbbr(stateName);
  const stateData = stateIndex[stateKey];
  if (stateData && stateData.cities.size > 0) {
    const firstCityKey = Array.from(stateData.cities)[0];
    const city = cityIndex[firstCityKey];
    if (city) {
      eventsMap.setView([city.lat, city.lng], 6);
    }
  }
}

// Helper: center map on first city of a country
function centerMapOnCountry(countryName) {
  const countryKey = countryName.toLowerCase();
  const countryData = countryIndex[countryKey];
  if (countryData && countryData.states.size > 0) {
    const firstState = Array.from(countryData.states)[0];
    const stData = stateIndex[firstState];
    if (stData && stData.cities.size > 0) {
      const firstCityKey = Array.from(stData.cities)[0];
      const city = cityIndex[firstCityKey];
      if (city) {
        eventsMap.setView([city.lat, city.lng], 4);
      }
    }
  }
}

// ═══════════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════════
document.addEventListener('DOMContentLoaded', async () => {
  initializeEventBuilder();
  initEventsMap();
  setupEventListeners();
  await loadEventsData();
});
