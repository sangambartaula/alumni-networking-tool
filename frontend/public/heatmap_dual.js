// heatmap_dual.js - Dual map system with both 2D Leaflet and 3D Cesium

// This 3D view uses public imagery providers only and does not bundle a Cesium Ion token.

let map2D;
let map3D;
let map3DInitialized = false;
let map3DFailureNotified = false;
window.map3DAvailable = true;
let locationClusters = [];
let currentMode = '2d'; // Start with 2D
let markers2D = [];
let entities3D = [];
let heatLayer2D = null; // New 2D heatmap layer
let activeHeatmapRequestToken = 0;
let currentDatasetServerFiltered = false;

// Filter state
let hiddenLocations = new Set();
let hiddenCompanies = new Set();
let selectedUntAlumniStatus = '';
let filterGradYearFrom = null;  // number | null
let filterGradYearTo   = null;  // number | null
let selectedHeatmapMajors = new Set();
let selectedHeatmapDegrees = new Set();
let selectedHeatmapSeniorities = new Set();

const HEATMAP_DEGREE_OPTIONS = ['Bachelors', 'Masters', 'PhD'];
const HEATMAP_SENIORITY_OPTIONS = ['Intern', 'Mid', 'Senior', 'Manager', 'Executive'];
const HEATMAP_ALLOWED_MAJORS = [
  'Artificial Intelligence',
  'Biomedical Engineering',
  'Computer Engineering',
  'Computer Science',
  'Construction Engineering Technology',
  'Construction Management',
  'Cybersecurity',
  'Data Engineering',
  'Electrical Engineering',
  'Engineering Management',
  'Geographic Information Systems + Computer Science',
  'Information Technology',
  'Materials Science and Engineering',
  'Mechanical and Energy Engineering',
  'Mechanical Engineering Technology',
  'Semiconductor Manufacturing Engineering',
];

function normalizeDegreeToFilterLabel(value) {
  const normalized = (value || '').trim().toLowerCase();
  if (!normalized) return '';
  if (normalized === 'undergraduate' || normalized === 'bachelors') return 'Bachelors';
  if (normalized === 'graduate' || normalized === 'masters') return 'Masters';
  if (normalized === 'phd') return 'PhD';
  return '';
}

function setInputWarning(warningEl, message) {
  if (!warningEl) return;
  warningEl.textContent = message || '';
}

function enforceIntegerOnlyInput(inputEl, warningEl) {
  if (!inputEl) return;

  inputEl.addEventListener('beforeinput', (e) => {
    if (e.data == null) return;
    if (e.data === '.') {
      e.preventDefault();
      setInputWarning(warningEl, 'Integer values only.');
      return;
    }
    if (!/^\d+$/.test(e.data)) {
      e.preventDefault();
      setInputWarning(warningEl, 'Integer values only.');
    }
  });

  inputEl.addEventListener('input', () => {
    const original = inputEl.value;
    const sanitized = original.replace(/[^\d]/g, '');
    if (sanitized !== original) {
      inputEl.value = sanitized;
      setInputWarning(warningEl, 'Integer values only.');
      return;
    }
    if (warningEl && warningEl.textContent === 'Integer values only.') {
      setInputWarning(warningEl, '');
    }
  });
}

function parseValidatedIntegerRange(minInputEl, maxInputEl, warningEl) {
  const minRaw = minInputEl ? minInputEl.value.trim() : '';
  const maxRaw = maxInputEl ? maxInputEl.value.trim() : '';
  const minVal = minRaw !== '' ? parseInt(minRaw, 10) : null;
  const maxVal = maxRaw !== '' ? parseInt(maxRaw, 10) : null;

  if (minVal != null && maxVal != null && minVal > maxVal) {
    setInputWarning(warningEl, 'Min cannot be greater than max.');
    return { valid: false, min: null, max: null };
  }

  if (warningEl && warningEl.textContent === 'Min cannot be greater than max.') {
    setInputWarning(warningEl, '');
  }

  return { valid: true, min: minVal, max: maxVal };
}

// Track all available locations and companies for autocomplete
let allLocations = new Set();
let allCompanies = new Set();

function showNotification(message, type = 'warning', timeout = 5000) {
  const heatmapSection = document.querySelector('.heatmap-section') || document.body;
  let container = document.querySelector('.map-notification-container');
  if (!container) {
    container = document.createElement('div');
    container.className = 'map-notification-container';
    heatmapSection.appendChild(container);
  }

  const notice = document.createElement('div');
  notice.className = `map-notification map-notification-${type}`;
  notice.innerHTML = `
    <span>${message}</span>
    <button type="button" class="map-notification-close" aria-label="Close notification">&times;</button>
  `;

  const closeNotice = () => {
    notice.classList.add('hiding');
    setTimeout(() => {
      if (notice.parentNode) notice.parentNode.removeChild(notice);
    }, 160);
  };

  notice.querySelector('.map-notification-close')?.addEventListener('click', closeNotice);
  container.appendChild(notice);
  setTimeout(closeNotice, timeout);
}

function updateToggleActiveButton() {
  const buttons = document.querySelectorAll('.view-mode-toggle .toggle-btn');
  buttons.forEach(btn => {
    btn.classList.toggle('active', btn.dataset.mode === currentMode);
  });
}

function update3DToggleAvailability() {
  const button3D = document.querySelector('.view-mode-toggle [data-mode="3d"]');
  if (!button3D) return;

  if (!window.map3DAvailable) {
    button3D.classList.add('unavailable');
    button3D.setAttribute('aria-disabled', 'true');
    button3D.title = '3D view not available (WebGL initialization failed).';
  } else {
    button3D.classList.remove('unavailable');
    button3D.removeAttribute('aria-disabled');
    button3D.title = 'Switch to 3D globe view';
  }
}

function mark3DUnavailable(error, notifyUser = true) {
  if (error) {
    console.error('3D map initialization failed:', error);
  }

  if (map3D && typeof map3D.destroy === 'function') {
    try {
      if (!map3D.isDestroyed || !map3D.isDestroyed()) {
        map3D.destroy();
      }
    } catch (destroyError) {
      console.error('Error destroying failed 3D viewer instance:', destroyError);
    }
  }

  map3D = null;
  map3DInitialized = false;
  window.map3DAvailable = false;
  currentMode = '2d';

  const map2DContainer = document.getElementById('map2DContainer');
  const map3DContainer = document.getElementById('map3DContainer');
  if (map2DContainer) map2DContainer.style.display = 'block';
  if (map3DContainer) map3DContainer.style.display = 'none';

  update3DToggleAvailability();
  updateToggleActiveButton();

  if (notifyUser && !map3DFailureNotified) {
    showNotification('3D visualization unavailable. Using 2D map instead.', 'warning');
    map3DFailureNotified = true;
  }
}

function ensure3DMapInitialized() {
  if (!window.map3DAvailable) return false;
  if (map3DInitialized && map3D) return true;

  if (!window.Cesium) {
    mark3DUnavailable(new Error('Cesium failed to load.'));
    return false;
  }

  const map3DContainer = document.getElementById('map3DContainer');
  if (!map3DContainer) {
    mark3DUnavailable(new Error('3D map container not found.'));
    return false;
  }

  try {
    initialize3DMap();
    map3DInitialized = true;
    window.map3DAvailable = true;
    update3DToggleAvailability();

    if (locationClusters.length > 0) {
      reloadMapData();
    }

    return true;
  } catch (error) {
    mark3DUnavailable(error);
    return false;
  }
}

// localStorage helper functions
function saveHiddenFiltersToStorage() {
  localStorage.setItem('hiddenLocations', JSON.stringify(Array.from(hiddenLocations)));
  localStorage.setItem('hiddenCompanies', JSON.stringify(Array.from(hiddenCompanies)));
  localStorage.setItem('heatmapUntAlumniStatus', selectedUntAlumniStatus || '');
  localStorage.setItem('heatmapGradYearFrom', filterGradYearFrom != null ? String(filterGradYearFrom) : '');
  localStorage.setItem('heatmapGradYearTo',   filterGradYearTo   != null ? String(filterGradYearTo)   : '');
  localStorage.setItem('heatmapSelectedMajors', JSON.stringify(Array.from(selectedHeatmapMajors)));
  localStorage.setItem('heatmapSelectedDegrees', JSON.stringify(Array.from(selectedHeatmapDegrees)));
  localStorage.setItem('heatmapSelectedSeniorities', JSON.stringify(Array.from(selectedHeatmapSeniorities)));
}

function loadHiddenFiltersFromStorage() {
  try {
    const savedLocations = JSON.parse(localStorage.getItem('hiddenLocations') || '[]');
    const savedCompanies = JSON.parse(localStorage.getItem('hiddenCompanies') || '[]');
    const savedUntAlumniStatus = localStorage.getItem('heatmapUntAlumniStatus') || '';
    const savedGradFrom = localStorage.getItem('heatmapGradYearFrom');
    const savedGradTo   = localStorage.getItem('heatmapGradYearTo');

    hiddenLocations = new Set(savedLocations);
    hiddenCompanies = new Set(savedCompanies);
    selectedUntAlumniStatus = savedUntAlumniStatus;
    const savedMajors = localStorage.getItem('heatmapSelectedMajors');
    if (savedMajors) selectedHeatmapMajors = new Set(JSON.parse(savedMajors));
    const savedDegrees = localStorage.getItem('heatmapSelectedDegrees');
    if (savedDegrees) {
      selectedHeatmapDegrees = new Set(
        JSON.parse(savedDegrees)
          .map(normalizeDegreeToFilterLabel)
          .filter(Boolean)
      );
    }
    const savedSeniorities = localStorage.getItem('heatmapSelectedSeniorities');
    if (savedSeniorities) selectedHeatmapSeniorities = new Set(JSON.parse(savedSeniorities));
    filterGradYearFrom = savedGradFrom ? (parseInt(savedGradFrom, 10) || null) : null;
    filterGradYearTo   = savedGradTo   ? (parseInt(savedGradTo,   10) || null) : null;
  } catch (e) {
    console.error('Error loading hidden filters from storage:', e);
    hiddenLocations = new Set();
    hiddenCompanies = new Set();
    selectedUntAlumniStatus = '';
    filterGradYearFrom = null;
    filterGradYearTo   = null;
    selectedHeatmapMajors = new Set();
    selectedHeatmapDegrees = new Set();
    selectedHeatmapSeniorities = new Set();
  }
}

function clearHiddenFiltersFromStorage() {
  localStorage.removeItem('hiddenLocations');
  localStorage.removeItem('hiddenCompanies');
  localStorage.removeItem('heatmapUntAlumniStatus');
  localStorage.removeItem('heatmapGradYearFrom');
  localStorage.removeItem('heatmapGradYearTo');
  localStorage.removeItem('heatmapSelectedMajors');
  localStorage.removeItem('heatmapSelectedDegrees');
  localStorage.removeItem('heatmapSelectedSeniorities');
}

function escapeAttribute(value) {
  return String(value).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function highlightSuggestionMatch(text, search) {
  if (!search) return escapeHtml(text);
  const escapedSearch = search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`(${escapedSearch})`, 'gi');
  return escapeHtml(text).replace(regex, '<strong>$1</strong>');
}

// Show autocomplete suggestions for location input
function showLocationSuggestions() {
  const input = document.getElementById('filterLocationInput');
  const suggestionBox = document.getElementById('locationSuggestionsDropdown');

  if (!input || !suggestionBox) return;

  const query = input.value.trim();

  if (query) {
    const valueLower = query.toLowerCase();
    const suggestions = Array.from(allLocations)
      .filter((loc) => loc.toLowerCase().includes(valueLower))
      .sort((a, b) => {
        const aLower = a.toLowerCase();
        const bLower = b.toLowerCase();
        if (aLower === valueLower) return -1;
        if (bLower === valueLower) return 1;
        if (aLower.startsWith(valueLower) && !bLower.startsWith(valueLower)) return -1;
        if (!aLower.startsWith(valueLower) && bLower.startsWith(valueLower)) return 1;
        return a.localeCompare(b);
      })
      .slice(0, 15);

    suggestionBox.innerHTML = suggestions.length > 0
      ? suggestions
          .map((loc) => `<div class="analytics-suggestion-item" data-value="${escapeAttribute(loc)}"><span class="analytics-suggestion-text">${highlightSuggestionMatch(loc, query)}</span></div>`)
          .join('')
      : '<div class="analytics-suggestion-no-results">No matching locations found</div>';
    suggestionBox.style.display = 'block';
    return;
  }

  const locationCounts = {};
  locationClusters.forEach((location) => {
    if (!location?.location) return;
    const visibleCount = getVisibleAlumniForLocation(location).length;
    if (visibleCount > 0) {
      locationCounts[location.location] = (locationCounts[location.location] || 0) + visibleCount;
    }
  });

  const topLocations = Object.entries(locationCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);

  if (topLocations.length === 0) {
    suggestionBox.innerHTML = '<div class="analytics-suggestion-no-results">No locations available</div>';
    suggestionBox.style.display = 'block';
    return;
  }

  suggestionBox.innerHTML =
    '<div class="analytics-suggestion-header">Popular Locations (click to hide)</div>' +
    topLocations
      .map(([location, count]) => `<div class="analytics-suggestion-item" data-value="${escapeAttribute(location)}"><span class="analytics-suggestion-text">${escapeHtml(location)}</span><span class="analytics-suggestion-count">(${count})</span></div>`)
      .join('');
  suggestionBox.style.display = 'block';
}

// Show autocomplete suggestions for company input
function showCompanySuggestions() {
  const input = document.getElementById('filterCompanyInput');
  const suggestionBox = document.getElementById('companySuggestionsDropdown');

  if (!input || !suggestionBox) return;

  const query = input.value.trim();

  if (query) {
    const valueLower = query.toLowerCase();
    const suggestions = Array.from(allCompanies)
      .filter((company) => company.toLowerCase().includes(valueLower))
      .sort((a, b) => {
        const aLower = a.toLowerCase();
        const bLower = b.toLowerCase();
        if (aLower === valueLower) return -1;
        if (bLower === valueLower) return 1;
        if (aLower.startsWith(valueLower) && !bLower.startsWith(valueLower)) return -1;
        if (!aLower.startsWith(valueLower) && bLower.startsWith(valueLower)) return 1;
        return a.localeCompare(b);
      })
      .slice(0, 15);

    suggestionBox.innerHTML = suggestions.length > 0
      ? suggestions
          .map((company) => `<div class="analytics-suggestion-item" data-value="${escapeAttribute(company)}"><span class="analytics-suggestion-text">${highlightSuggestionMatch(company, query)}</span></div>`)
          .join('')
      : '<div class="analytics-suggestion-no-results">No matching companies found</div>';
    suggestionBox.style.display = 'block';
    return;
  }

  const companyCounts = {};
  locationClusters.forEach((location) => {
    (location?.sample_alumni || []).forEach((alumni) => {
      if (!alumni?.company || isCompanyHidden(alumni)) return;
      companyCounts[alumni.company] = (companyCounts[alumni.company] || 0) + 1;
    });
  });

  const topCompanies = Object.entries(companyCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);

  if (topCompanies.length === 0) {
    suggestionBox.innerHTML = '<div class="analytics-suggestion-no-results">No companies available</div>';
    suggestionBox.style.display = 'block';
    return;
  }

  suggestionBox.innerHTML =
    '<div class="analytics-suggestion-header">Popular Companies (click to hide)</div>' +
    topCompanies
      .map(([company, count]) => `<div class="analytics-suggestion-item" data-value="${escapeAttribute(company)}"><span class="analytics-suggestion-text">${escapeHtml(company)}</span><span class="analytics-suggestion-count">(${count})</span></div>`)
      .join('');
  suggestionBox.style.display = 'block';
}

// Extract all locations and companies from alumni data for autocomplete
function buildAutocompleteData(alumniData) {
  allLocations.clear();
  allCompanies.clear();

  if (Array.isArray(alumniData)) {
    alumniData.forEach(alumni => {
      if (alumni.location) {
        allLocations.add(alumni.location);
      }
      if (alumni.company) {
        allCompanies.add(alumni.company);
      }
    });
  }
}

// Fix viewport layout for full-screen map
function setupMapViewport() {
  const resizeMap = () => {
    const mapWrapper = document.querySelector('.map-wrapper');
    const mapContainer2D = document.getElementById('map2DContainer');
    const mapContainer3D = document.getElementById('map3DContainer');

    if (mapWrapper && mapContainer2D && mapContainer3D) {
      // Force refresh of Leaflet map size
      if (map2D && map2D.invalidateSize) {
        map2D.invalidateSize(false);
      }

      // Force refresh of Cesium map size
      if (map3D && map3D.scene) {
        map3D.scene.requestRender();
      }
    }
  };

  // Initial resize after a short delay to allow DOM to settle
  setTimeout(resizeMap, 100);

  // Resize on window resize
  window.addEventListener('resize', resizeMap);
}

// Grad-year filter applied from Analytics → Heatmap redirect
let heatmapGradYearFrom = null;
let heatmapGradYearTo   = null;

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
  // ── Read graduation year range from query string ──────────────────────────
  const urlParams = new URLSearchParams(window.location.search);
  const rawFrom = urlParams.get('from');
  const rawTo   = urlParams.get('to');

  if (rawFrom) heatmapGradYearFrom = parseInt(rawFrom, 10) || null;
  if (rawTo)   heatmapGradYearTo   = parseInt(rawTo,   10) || null;

  // Also feed URL params into the filter-panel state so inputs pre-populate
  if (rawFrom) filterGradYearFrom = heatmapGradYearFrom;
  if (rawTo)   filterGradYearTo   = heatmapGradYearTo;

  if (heatmapGradYearFrom != null || heatmapGradYearTo != null) {
    showGradYearBanner(heatmapGradYearFrom, heatmapGradYearTo);
  }

  setupMapViewport();
  initialize2DMap();
  add2D3DToggle();
  addLayerControls();
  addCustomFullscreenButton();

  if (!window.Cesium) {
    mark3DUnavailable(new Error('Cesium library not available.'));
  }

  // Build initial heatmap URL; buildHeatmapUrl() also picks up URL-seeded
  // filterGradYearFrom/To set above.
  loadHeatmapData(buildHeatmapUrl());
});

/**
 * Show a top-of-page banner indicating which grad year range is active.
 * Includes a dismiss button and a link back to Analytics.
 */
function showGradYearBanner(from, to) {
  const existing = document.getElementById('gradYearFilterBanner');
  if (existing) existing.remove();

  let rangeText;
  if (from != null && to != null) rangeText = `${from} – ${to}`;
  else if (from != null)          rangeText = `${from} and later`;
  else                            rangeText = `up to ${to}`;

  const banner = document.createElement('div');
  banner.id = 'gradYearFilterBanner';
  banner.className = 'grad-year-filter-banner';
  banner.innerHTML = `
    <span class="grad-year-banner-icon"><i class="fas fa-filter"></i></span>
    <span>Showing alumni who graduated&nbsp;<strong>${rangeText}</strong></span>
    <a href="/analytics" class="grad-year-banner-link"><i class="fas fa-chart-line"></i> Back to Analytics</a>
    <button class="grad-year-banner-close" title="Clear filter" onclick="clearGradYearFilter()">×</button>
  `;

  // Insert right after the <header>
  const header = document.querySelector('header.appbar');
  if (header && header.nextSibling) {
    header.parentNode.insertBefore(banner, header.nextSibling);
  } else {
    document.body.insertBefore(banner, document.body.firstChild);
  }
}

/** Remove the grad year filter and reload without params */
function clearGradYearFilter() {
  heatmapGradYearFrom = null;
  heatmapGradYearTo   = null;
  filterGradYearFrom  = null;
  filterGradYearTo    = null;
  const gradFromEl = document.getElementById('heatmapGradYearFrom');
  const gradToEl   = document.getElementById('heatmapGradYearTo');
  if (gradFromEl) gradFromEl.value = '';
  if (gradToEl)   gradToEl.value   = '';
  const banner = document.getElementById('gradYearFilterBanner');
  if (banner) banner.remove();
  saveHiddenFiltersToStorage();
  updateFilterBadge();
  window.history.replaceState({}, '', '/heatmap');
  reloadMapData();
}


// Initialize 2D Leaflet Map
function initialize2DMap() {
  map2D = L.map('map2DContainer', {
    center: [20, 0],
    zoom: 2,
    zoomControl: true
  });

  // Add Google Earth View (satellite + labels)
  L.tileLayer('https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
    attribution: 'Google Earth',
    maxZoom: 20
  }).addTo(map2D);
}

// Convert Leaflet zoom to Cesium altitude
function getAltitudeFromZoom(zoom) {
  return Math.pow(2, 21 - zoom) * 156543.03392;
}

// Convert altitude to Leaflet zoom level
function getZoomFromAltitude(altitude) {
  const zoom = Math.round(21 - Math.log2(altitude / 156543.03392));
  return Math.max(1, Math.min(20, zoom));
}

// Initialize 3D Cesium Globe
function initialize3DMap() {
  map3D = new Cesium.Viewer('map3DContainer', {
    animation: false,
    baseLayerPicker: false,
    fullscreenButton: false,
    geocoder: false,
    homeButton: false,
    infoBox: true,
    sceneModePicker: false,
    selectionIndicator: true,
    timeline: false,
    navigationHelpButton: false,
    navigationInstructionsInitiallyVisible: false,
    baseLayer: false
  });

  // Remove all default layers first
  map3D.imageryLayers.removeAll();

  // Add Google satellite imagery
  map3D.imageryLayers.addImageryProvider(
    new Cesium.UrlTemplateImageryProvider({
      url: 'https://mt{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
      subdomains: ['0', '1', '2', '3'],
      maximumLevel: 20,
      credit: 'Google'
    })
  );

  // Add labels overlay
  map3D.imageryLayers.addImageryProvider(
    new Cesium.UrlTemplateImageryProvider({
      url: 'https://mt{s}.google.com/vt/lyrs=h&x={x}&y={y}&z={z}',
      subdomains: ['0', '1', '2', '3'],
      maximumLevel: 20,
      credit: 'Google'
    })
  );

  // Configure scene for better rendering
  map3D.scene.globe.enableLighting = true;
  map3D.scene.globe.showGroundAtmosphere = true;
  map3D.scene.globe.atmosphereLightIntensity = 10.0;
  map3D.scene.globe.atmosphereSaturationShift = 0.0;
  map3D.scene.globe.atmosphereHueShift = 0.0;
  map3D.scene.skyBox.show = true;
  map3D.scene.moon.show = true;  // Show the moon
  map3D.scene.sun.show = true;
  map3D.scene.globe.show = true;
  map3D.scene.globe.baseColor = Cesium.Color.WHITE;
  map3D.scene.requestRenderMode = false;
  map3D.scene.highDynamicRange = true;

  // Set initial camera view
  map3D.camera.setView({
    destination: Cesium.Cartesian3.fromDegrees(0, 20, 20000000),
    orientation: {
      heading: 0.0,
      pitch: Cesium.Math.toRadians(-90),
      roll: 0.0
    }
  });

  // Enable camera controls
  map3D.scene.screenSpaceCameraController.enableRotate = true;
  map3D.scene.screenSpaceCameraController.enableZoom = true;
  map3D.scene.screenSpaceCameraController.enableTilt = true;
  map3D.scene.screenSpaceCameraController.enableLook = true;

  // Set minimum and maximum zoom distances
  map3D.scene.screenSpaceCameraController.minimumZoomDistance = 1000;
  map3D.scene.screenSpaceCameraController.maximumZoomDistance = 50000000;

  // Handle entity selection and make camera icon work
  map3D.selectedEntityChanged.addEventListener(function () {
    const entity = map3D.selectedEntity;
    if (entity && entity.position) {
      setTimeout(() => {
        const zoomFunction = function (e) {
          if (e) {
            e.preventDefault();
            e.stopPropagation();
          }

          // Get current entity's position dynamically
          const currentEntity = map3D.selectedEntity;
          if (currentEntity && currentEntity.position) {
            const position = currentEntity.position.getValue(Cesium.JulianDate.now());
            const cartographic = Cesium.Cartographic.fromCartesian(position);
            const longitude = Cesium.Math.toDegrees(cartographic.longitude);
            const latitude = Cesium.Math.toDegrees(cartographic.latitude);

            console.log('Flying to:', latitude, longitude);

            map3D.camera.flyTo({
              destination: Cesium.Cartesian3.fromDegrees(longitude, latitude, 200),
              orientation: {
                heading: 0,
                pitch: Cesium.Math.toRadians(-20),
                roll: 0
              },
              duration: 2
            });
          }
        };

        // Try multiple selectors for the camera button
        const cameraBtn = document.querySelector('.cesium-infoBox-camera') ||
          document.querySelector('button[title*="camera"]') ||
          document.querySelector('button[title*="Camera"]');

        const titleBar = document.querySelector('.cesium-infoBox-title');
        const infoBoxContainer = document.querySelector('.cesium-infoBox');

        console.log('Camera button found:', !!cameraBtn);
        console.log('Title bar found:', !!titleBar);

        // Attach to camera button if it exists
        if (cameraBtn) {
          cameraBtn.style.pointerEvents = 'auto';
          cameraBtn.style.cursor = 'pointer';
          cameraBtn.style.zIndex = '9999';
          cameraBtn.onclick = zoomFunction;
          console.log('Camera button handler attached');
        }

        // Make the title bar clickable
        if (titleBar) {
          titleBar.style.cursor = 'pointer';
          titleBar.style.pointerEvents = 'auto';
          titleBar.onclick = zoomFunction;
          console.log('Title bar handler attached');
        }

        // Make entire infobox header clickable as fallback
        if (infoBoxContainer) {
          const header = infoBoxContainer.querySelector('.cesium-infoBox-titlebar');
          if (header) {
            header.style.cursor = 'pointer';
            header.onclick = zoomFunction;
            console.log('Header handler attached');
          }
        }
      }, 500);
    }
  });
}

// Add 2D/3D toggle button
function add2D3DToggle() {
  const toggleDiv = document.createElement('div');
  toggleDiv.className = 'view-mode-toggle';
  toggleDiv.innerHTML = `
    <div class="toggle-panel">
      <button class="toggle-btn active" data-mode="2d">
        <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor">
          <path d="M20.5 3l-.16.03L15 5.1 9 3 3.36 4.9c-.21.07-.36.25-.36.48V20.5c0 .28.22.5.5.5l.16-.03L9 18.9l6 2.1 5.64-1.9c.21-.07.36-.25-.36-.48V3.5c0-.28-.22-.5-.5-.5zM15 19l-6-2.11V5l6 2.11V19z"/>
        </svg>
        <span>2D Map</span>
      </button>
      <button class="toggle-btn" data-mode="3d">
        <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor">
          <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/>
        </svg>
        <span>3D Globe</span>
      </button>
    </div>
  `;

  document.querySelector('.heatmap-section').appendChild(toggleDiv);

  const buttons = toggleDiv.querySelectorAll('.toggle-btn');
  update3DToggleAvailability();
  updateToggleActiveButton();

  buttons.forEach(btn => {
    btn.addEventListener('click', () => {
      const mode = btn.dataset.mode;
      switchMapMode(mode);
      updateToggleActiveButton();
    });
  });
}

// Switch between 2D and 3D maps
function switchMapMode(mode) {
  const map2DContainer = document.getElementById('map2DContainer');
  const map3DContainer = document.getElementById('map3DContainer');
  const cesiumToolbar = document.querySelector('.cesium-viewer-toolbar');

  if (mode === '3d') {
    if (!window.map3DAvailable) {
      showNotification('3D view not supported on this browser. Staying on 2D map.', 'warning');
      currentMode = '2d';
      if (map2DContainer) map2DContainer.style.display = 'block';
      if (map3DContainer) map3DContainer.style.display = 'none';
      return false;
    }

    if (map2DContainer) map2DContainer.style.display = 'none';
    if (map3DContainer) map3DContainer.style.display = 'block';

    if (!ensure3DMapInitialized()) {
      if (map2DContainer) map2DContainer.style.display = 'block';
      if (map3DContainer) map3DContainer.style.display = 'none';
      currentMode = '2d';
      return false;
    }

    currentMode = '3d';

    if (cesiumToolbar) {
      cesiumToolbar.style.display = 'block';
    }

    setTimeout(() => {
      if (map3D) {
        map3D.resize();
      }
    }, 100);

    return true;
  }

  currentMode = '2d';

  if (mode === '2d') {
    if (map2DContainer) map2DContainer.style.display = 'block';
    if (map3DContainer) map3DContainer.style.display = 'none';

    // Show Cesium fullscreen button for 2D mode too
    if (cesiumToolbar) {
      cesiumToolbar.style.display = 'block';
    }

    // Refresh Leaflet map
    setTimeout(() => {
      if (map2D) {
        map2D.invalidateSize();
      }
    }, 100);
  }

  return true;
}

// Toggle fullscreen for the entire heatmap section
function toggleFullscreen() {
  const heatmapSection = document.querySelector('.heatmap-section');

  if (!document.fullscreenElement) {
    heatmapSection.requestFullscreen().then(() => {
      setTimeout(() => {
        if (currentMode === '2d') {
          map2D.invalidateSize();
        } else if (map3D) {
          map3D.resize();
        }
      }, 100);
    }).catch(err => {
      console.error('Error attempting to enable fullscreen:', err);
    });
  } else {
    document.exitFullscreen().then(() => {
      setTimeout(() => {
        if (currentMode === '2d') {
          map2D.invalidateSize();
        } else if (map3D) {
          map3D.resize();
        }
      }, 100);
    });
  }
}

// Add custom layer controls
function addLayerControls() {
  const controlDiv = document.createElement('div');
  controlDiv.className = 'layer-control';
  controlDiv.style.display = 'block';
  controlDiv.style.visibility = 'visible';
  controlDiv.style.opacity = '1';
  controlDiv.innerHTML = `
    <div class="layer-control-panel">
      <div class="layer-control-title">Map Layers</div>
      <label class="layer-option">
        <input type="radio" name="layer" value="satellite" checked>
        <span>Google Earth View</span>
      </label>
      <label class="layer-option">
        <input type="radio" name="layer" value="terrain">
        <span>Terrain Map</span>
      </label>
    </div>
  `;

  document.querySelector('.heatmap-section').appendChild(controlDiv);
  console.log('Layer controls added:', controlDiv);

  // Handle layer switching
  const radios = controlDiv.querySelectorAll('input[type="radio"]');
  radios.forEach(radio => {
    radio.addEventListener('change', (e) => {
      const layerType = e.target.value;

      if (currentMode === '2d') {
        // Switch 2D Leaflet tiles
        map2D.eachLayer(layer => {
          if (layer instanceof L.TileLayer) {
            map2D.removeLayer(layer);
          }
        });

        if (layerType === 'satellite') {
          L.tileLayer('https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
            maxZoom: 20
          }).addTo(map2D);
        } else {
          L.tileLayer('https://mt1.google.com/vt/lyrs=p&x={x}&y={y}&z={z}', {
            maxZoom: 20
          }).addTo(map2D);
        }

        // Re-add markers
        markers2D.forEach(marker => map2D.addLayer(marker));

      } else {
        if (!map3DInitialized || !map3D) {
          showNotification('3D layer controls are unavailable until 3D view initializes.', 'warning');
          switchMapMode('2d');
          updateToggleActiveButton();
          return;
        }

        // Switch 3D Cesium imagery
        map3D.imageryLayers.removeAll();

        if (layerType === 'satellite') {
          map3D.imageryLayers.addImageryProvider(
            new Cesium.UrlTemplateImageryProvider({
              url: 'https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
              maximumLevel: 20
            })
          );
          map3D.imageryLayers.addImageryProvider(
            new Cesium.UrlTemplateImageryProvider({
              url: 'https://mt1.google.com/vt/lyrs=h&x={x}&y={y}&z={z}',
              maximumLevel: 20
            })
          );
        } else {
          map3D.imageryLayers.addImageryProvider(
            new Cesium.UrlTemplateImageryProvider({
              url: 'https://mt1.google.com/vt/lyrs=p&x={x}&y={y}&z={z}',
              maximumLevel: 20
            })
          );
        }
      }
    });
  });
}

// Load heatmap data
// Load heatmap data
async function loadHeatmapData(url = '/api/heatmap') {
  const requestToken = ++activeHeatmapRequestToken;
  try {
    const separator = url.includes('?') ? '&' : '?';
    const fetchUrl = url + separator + '_t=' + new Date().getTime();
    const response = await fetch(fetchUrl);
    const data = await response.json();

    // Ignore out-of-order responses from older requests.
    if (requestToken !== activeHeatmapRequestToken) {
      return;
    }

    const parsedUrl = new URL(url, window.location.origin);
    const query = parsedUrl.searchParams;
    currentDatasetServerFiltered = (
      query.has('unt_alumni_status')
      || query.has('grad_year_from')
      || query.has('grad_year_to')
      || query.has('standardized_major')
      || query.has('degree')
      || query.has('seniority')
    );

    if (!data.success) {
      showError('Failed to load heatmap data');
      return;
    }

    locationClusters = data.locations || [];

    if (!locationClusters || locationClusters.length === 0) {
      showError('No alumni with coordinates found.');
      return;
    }

    // Clear old markers/heat if this is a reload
    markers2D.forEach(m => map2D.removeLayer(m));
    markers2D = [];
    if (heatLayer2D) {
      map2D.removeLayer(heatLayer2D);
      heatLayer2D = null;
    }
    if (window.map3DAvailable && map3D) {
      entities3D.forEach(e => map3D.entities.remove(e));
    }
    entities3D = [];

    // Add markers to both maps
    locationClusters.forEach(location => {
      add2DMarker(location);
      if (window.map3DAvailable && map3D) {
        add3DMarker(location);
      }
    });

    // NEW: build 2D heatmap layer (weather-style)
    render2DHeatmap(locationClusters, data.max_count);

    // Update statistics
    updateStatistics(data.total_alumni, locationClusters.length, {
      totalFilteredAlumni: data.total_filtered_alumni,
      missingLocationCount: data.missing_location_count,
      ungeocodedWithLocationCount: data.ungeocoded_with_location_count,
    });

    // Build autocomplete data from all locations and sample alumni
    const allAlumniForAutocomplete = [];
    locationClusters.forEach(location => {
      allAlumniForAutocomplete.push({
        location: location.location,
        company: location.sample_alumni.length > 0 ? location.sample_alumni[0].company : null
      });
      if (location.sample_alumni) {
        location.sample_alumni.forEach(alumni => {
          allAlumniForAutocomplete.push({
            location: location.location,
            company: alumni.company
          });
        });
      }
    });
    buildAutocompleteData(allAlumniForAutocomplete);

    // Initialize filter UI after data is loaded
    initializeFilterUI();

  } catch (error) {
    console.error('Error loading heatmap data:', error);
    showError('Error loading heatmap data: ' + error.message);
  }
}


// Add marker to 2D Leaflet map
function add2DMarker(location) {
  const marker = L.circleMarker([location.latitude, location.longitude], {
    radius: 8 + Math.sqrt(location.count) * 2,
    fillColor: getColorForCount(location.count),
    color: '#fff',
    weight: 2,
    opacity: 1,
    fillOpacity: 0.8
  });

  // Create popup content
  const popupContent = create2DPopupContent(location);
  marker.bindPopup(popupContent, { maxWidth: 350 });

  marker.addTo(map2D);
  markers2D.push(marker);
}

// Add marker to 3D Cesium map
function add3DMarker(location) {
  if (!window.map3DAvailable || !map3D) return;

  const entity = map3D.entities.add({
    name: location.location || 'Unknown Location',
    position: Cesium.Cartesian3.fromDegrees(location.longitude, location.latitude, 0),
    point: {
      pixelSize: 15 + Math.sqrt(location.count) * 3,
      color: getCesiumColorForCount(location.count),
      outlineColor: Cesium.Color.WHITE,
      outlineWidth: 2,
      heightReference: Cesium.HeightReference.CLAMP_TO_GROUND
    },
    label: {
      text: location.count.toString(),
      font: 'bold 14px sans-serif',
      fillColor: Cesium.Color.WHITE,
      outlineColor: Cesium.Color.BLACK,
      outlineWidth: 2,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      verticalOrigin: Cesium.VerticalOrigin.CENTER,
      horizontalOrigin: Cesium.HorizontalOrigin.CENTER,
      pixelOffset: new Cesium.Cartesian2(0, 0),
      heightReference: Cesium.HeightReference.CLAMP_TO_GROUND
    },
    description: create3DPopupContent(location)
  });

  entities3D.push(entity);
}

// Build Leaflet heat layer from clustered data
function render2DHeatmap(locations, backendMaxCount) {
  if (!map2D || !locations || locations.length === 0) return;

  // Use max from backend if provided, else compute here
  let maxCount = backendMaxCount || 0;
  if (!maxCount) {
    maxCount = Math.max(...locations.map(l => l.count || 1));
  }
  if (!maxCount || maxCount <= 0) maxCount = 1;

  // Convert to [lat, lon, intensity] for Leaflet.heat
  const heatPoints = locations.map(l => {
    const intensity = (l.count || 1) / maxCount; // 0–1
    return [l.latitude, l.longitude, intensity];
  });

  // Remove previous layer if any
  if (heatLayer2D) {
    map2D.removeLayer(heatLayer2D);
  }

  // Create new heat layer
  heatLayer2D = L.heatLayer(heatPoints, {
    radius: 45,    // bigger bubble → more "weather radar" look
    blur: 30,
    maxZoom: 8,
    // Gradient: blue → green → yellow → orange → red
    gradient: {
      0.2: '#00bfff',
      0.4: '#00ff7f',
      0.6: '#ffff00',
      0.8: '#ff8c00',
      1.0: '#ff0000'
    }
  }).addTo(map2D);
}


// Create popup content for 2D map
function create2DPopupContent(location) {
  // Filter alumni to exclude hidden companies
  const visibleAlumni = location.sample_alumni.filter(a => !isCompanyHidden(a));

  const alumniItems = visibleAlumni
    .map(a => `
      <div style="padding: 10px; border-bottom: 1px solid #eee; background: #f9f9f9; margin-bottom: 5px; border-radius: 4px;">
        <div style="font-weight: 600; color: #333; font-size: 14px;">${a.name}</div>
        <div style="color: #666; font-size: 13px; margin-top: 4px;">${a.company || 'Company not specified'}</div>
        <div style="color: #888; font-size: 12px; margin-top: 2px;">${a.position || a.role || 'Position not specified'}</div>
        ${a.linkedin ? `<a href="${a.linkedin}" target="_blank" style="color: #0a66c2; font-size: 12px; text-decoration: none; display: inline-block; margin-top: 4px;">View LinkedIn Profile →</a>` : ''}
      </div>
    `)
    .join('');

  return `
    <div style="max-width: 350px; font-family: Arial, sans-serif;">
      <h3 style="margin: 0 0 12px 0; color: #667eea; border-bottom: 2px solid #667eea; padding-bottom: 8px;">${location.location}</h3>
      <div style="margin-bottom: 12px; padding: 8px; background: #e8f0fe; border-radius: 4px;">
        <strong style="color: #333;">Total Alumni:</strong> <span style="color: #667eea; font-weight: 600;">${visibleAlumni.length}</span>
      </div>
      <div style="max-height: 400px; overflow-y: auto;">
        ${alumniItems || '<p style="color: #999; font-style: italic;">All alumni in this location are hidden by filters</p>'}
      </div>
    </div>
  `;
}

// Create popup content for 3D map
function create3DPopupContent(location) {
  // Filter alumni to exclude hidden companies
  const visibleAlumni = location.sample_alumni.filter(a => !isCompanyHidden(a));

  const alumniItems = visibleAlumni
    .map(a => `
      <div style="padding: 10px; border-bottom: 1px solid #eee; background: #f9f9f9; margin-bottom: 5px; border-radius: 4px;">
        <div style="font-weight: 600; color: #333; font-size: 14px;">${a.name}</div>
        <div style="color: #666; font-size: 13px; margin-top: 4px;">${a.company || 'Company not specified'}</div>
        <div style="color: #888; font-size: 12px; margin-top: 2px;">${a.position || a.role || 'Position not specified'}</div>
        ${a.linkedin ? `<a href="${a.linkedin}" target="_blank" style="color: #0a66c2; font-size: 12px; text-decoration: none; display: inline-block; margin-top: 4px;">View LinkedIn Profile →</a>` : ''}
      </div>
    `)
    .join('');

  return `
    <div style="max-width: 350px; font-family: Arial, sans-serif;">
      <div style="margin-bottom: 12px; padding: 8px; background: #e8f0fe; border-radius: 4px;">
        <strong style="color: #333;">Total Alumni:</strong> <span style="color: #667eea; font-weight: 600;">${visibleAlumni.length}</span>
      </div>
      <div style="max-height: 400px; overflow-y: auto;">
        ${alumniItems || '<p style="color: #999; font-style: italic;">All alumni in this location are hidden by filters</p>'}
      </div>
    </div>
  `;
}

// Get color for count (Leaflet)
function getColorForCount(count) {
  if (count >= 10) return '#FF4444';
  if (count >= 7) return '#FF8C00';
  if (count >= 5) return '#FFD700';
  if (count >= 3) return '#32CD32';
  return '#00CED1';
}

// Get color for count (Cesium)
function getCesiumColorForCount(count) {
  if (count >= 10) return Cesium.Color.RED;
  if (count >= 7) return Cesium.Color.ORANGE;
  if (count >= 5) return Cesium.Color.YELLOW;
  if (count >= 3) return Cesium.Color.LIME;
  return Cesium.Color.CYAN;
}

// Reset to Earth view
function resetContinent() {
  if (currentMode === '2d') {
    // Reset 2D map
    map2D.setView([20, 0], 2);
  } else if (map3DInitialized && map3D) {
    // Return to default Earth view in 3D
    map3D.camera.flyTo({
      destination: Cesium.Cartesian3.fromDegrees(0, 20, 20000000),
      orientation: {
        heading: 0.0,
        pitch: Cesium.Math.toRadians(-90),
        roll: 0.0
      },
      duration: 2
    });
  } else {
    showNotification('3D view is unavailable. Showing 2D map.', 'warning');
    switchMapMode('2d');
    updateToggleActiveButton();
  }
}

// Filter by continent
function filterByContinent(continentName) {
  const bounds = {
    'North America': [[15, -170], [72, -50]],
    'South America': [[-56, -82], [13, -34]],
    'Europe': [[36, -10], [71, 40]],
    'Asia': [[0, 40], [55, 150]],
    'Africa': [[-35, -20], [37, 52]],
    'Oceania': [[-47, 110], [-10, 180]],
    'Antarctica': [[-90, -180], [-60, 180]]
  };

  if (currentMode === '2d' && bounds[continentName]) {
    map2D.fitBounds(bounds[continentName]);
  } else if (currentMode === '3d' && bounds[continentName] && map3DInitialized && map3D) {
    const [[minLat, minLng], [maxLat, maxLng]] = bounds[continentName];
    const centerLat = (minLat + maxLat) / 2;
    const centerLng = (minLng + maxLng) / 2;

    map3D.camera.flyTo({
      destination: Cesium.Cartesian3.fromDegrees(centerLng, centerLat, 5000000),
      duration: 2
    });
  } else if (currentMode === '3d') {
    showNotification('3D view is unavailable. Showing 2D map.', 'warning');
    switchMapMode('2d');
    updateToggleActiveButton();
  }
}

// Update statistics
function updateStatistics(totalAlumni, uniqueLocations, details = {}) {
  const statsDiv = document.getElementById('heatmapStats');
  const totalFiltered = Number(details.totalFilteredAlumni || totalAlumni || 0);
  const missingLocationCount = Number(details.missingLocationCount || 0);
  const ungeocodedWithLocationCount = Number(details.ungeocodedWithLocationCount || 0);
  const notPlotted = Math.max(totalFiltered - totalAlumni, 0);
  const qualityTooltip = `Not plotted: ${notPlotted} | Location not found: ${missingLocationCount} | Has location but not geocoded: ${ungeocodedWithLocationCount}`;

  statsDiv.innerHTML = `
    <div class="stat-item">
      <span class="stat-label" title="${qualityTooltip}">Alumni Plotted (Geocoded):</span>
      <span class="stat-value">${totalAlumni}</span>
    </div>
    <div class="stat-item">
      <span class="stat-label">Unique Locations:</span>
      <span class="stat-value">${uniqueLocations}</span>
    </div>
  `;
}

// Show error message
function showError(message) {
  console.error(message);
  const statsDiv = document.getElementById('heatmapStats');
  if (statsDiv) {
    statsDiv.innerHTML = `<p style="color: #e74c3c;">${message}</p>`;
  }
}

// Aggregate alumni by continent
function aggregateAlumniByContinent(locations) {
  const continentCounts = {
    "North America": 0,
    "South America": 0,
    "Europe": 0,
    "Asia": 0,
    "Africa": 0,
    "Oceania": 0,
    "Antarctica": 0
  };

  locations.forEach(location => {
    const lat = location.latitude;
    const lon = location.longitude;

    if (lat >= 10 && lat <= 83 && lon >= -170 && lon <= -50) {
      continentCounts["North America"] += location.count;
    } else if (lat >= -56 && lat <= 13 && lon >= -81 && lon <= -34) {
      continentCounts["South America"] += location.count;
    } else {
      continentCounts["North America"] += location.count;
    }
  });

  return continentCounts;
}

// Manual header toggle functionality
function setupHeaderToggle() {
  const header = document.getElementById('heatmapHeader');
  const toggleBtn = document.getElementById('headerToggleBtn');
  let isHeaderVisible = true;

  toggleBtn.addEventListener('click', () => {
    isHeaderVisible = !isHeaderVisible;

    if (isHeaderVisible) {
      header.classList.remove('hidden');
      toggleBtn.classList.remove('rotated');
    } else {
      header.classList.add('hidden');
      toggleBtn.classList.add('rotated');
    }
  });
}

// Fullscreen toggle
function setupFullscreenToggle() {
  const fullscreenBtn = document.getElementById('fullscreenBtn');
  const heatmapSection = document.querySelector('.heatmap-section');
  const fullscreenIcon = fullscreenBtn.querySelector('.fullscreen-icon');
  const exitFullscreenIcon = fullscreenBtn.querySelector('.exit-fullscreen-icon');

  fullscreenBtn.addEventListener('click', () => {
    heatmapSection.classList.toggle('fullscreen-mode');

    if (heatmapSection.classList.contains('fullscreen-mode')) {
      fullscreenIcon.style.display = 'none';
      exitFullscreenIcon.style.display = 'block';
      document.body.style.overflow = 'hidden';
    } else {
      fullscreenIcon.style.display = 'block';
      exitFullscreenIcon.style.display = 'none';
      document.body.style.overflow = '';
    }

    setTimeout(() => {
      if (currentMode === '2d') {
        map2D.invalidateSize();
      } else if (map3D) {
        map3D.resize();
      }
    }, 300);
  });
}

// Add custom fullscreen button that works for both 2D and 3D maps
function addCustomFullscreenButton() {
  const fullscreenDiv = document.createElement('div');
  fullscreenDiv.className = 'cesium-fullscreen-wrapper';
  fullscreenDiv.innerHTML = `
    <button class="cesium-button cesium-toolbar-button" title="Toggle Fullscreen">
      <svg viewBox="0 0 28 28" width="14" height="14">
        <path d="M4,11V4h7 M24,11V4h-7 M4,17v7h7 M24,17v7h-7" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
      </svg>
    </button>
  `;

  document.querySelector('.heatmap-section').appendChild(fullscreenDiv);

  const btn = fullscreenDiv.querySelector('.cesium-button');
  btn.addEventListener('click', () => {
    toggleFullscreen();
  });

  // Update icon on fullscreen change
  document.addEventListener('fullscreenchange', () => {
    if (document.fullscreenElement) {
      btn.innerHTML = `
        <svg viewBox="0 0 28 28" width="14" height="14">
          <path d="M11,4v7H4 M17,4v7h7 M11,24v-7H4 M17,24v-7h7" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
        </svg>
      `;
    } else {
      btn.innerHTML = `
        <svg viewBox="0 0 28 28" width="14" height="14">
          <path d="M4,11V4h7 M24,11V4h-7 M4,17v7h7 M24,17v7h-7" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
        </svg>
      `;
    }
  });
}

/* -----------------------------
   SEARCH + AUTOCOMPLETE (Option B)
   Alumni matches + Nominatim
------------------------------ */

// Zoom to alumni location (already used elsewhere)
function zoomToLocation(loc) {
  if (currentMode === '2d') {
    map2D.setView([loc.latitude, loc.longitude], 8);
  } else if (map3DInitialized && map3D) {
    map3D.camera.flyTo({
      destination: Cesium.Cartesian3.fromDegrees(loc.longitude, loc.latitude, 2000000),
      duration: 2
    });
  } else {
    showNotification('3D view is unavailable. Showing 2D map.', 'warning');
    switchMapMode('2d');
    updateToggleActiveButton();
    map2D.setView([loc.latitude, loc.longitude], 8);
  }
}

// Zoom to selected autocomplete result from Nominatim
function zoomToSearchLocation(item) {
  const lat = parseFloat(item.lat);
  const lon = parseFloat(item.lon);

  if (currentMode === '2d') {
    map2D.setView([lat, lon], 8);
  } else if (map3DInitialized && map3D) {
    map3D.camera.flyTo({
      destination: Cesium.Cartesian3.fromDegrees(lon, lat, 2000000),
      duration: 2
    });
  } else {
    showNotification('3D view is unavailable. Showing 2D map.', 'warning');
    switchMapMode('2d');
    updateToggleActiveButton();
    map2D.setView([lat, lon], 8);
  }
}

// DOM elements
const searchInput = document.getElementById('locationSearchInput');
const suggestionsBox = document.getElementById('locationSuggestions');
const searchButton = document.getElementById('locationSearchButton');

// Keep suggestions open while clicking inside
if (suggestionsBox) {
  suggestionsBox.addEventListener('mousedown', (e) => {
    e.preventDefault();
  });
}

// Fetch Nominatim suggestions via your backend /api/geocode
async function fetchGeocodeSuggestions(query) {
  if (!query || query.length < 2) return [];
  try {
    const res = await fetch(`/api/geocode?q=${encodeURIComponent(query)}`);
    const data = await res.json();
    if (!data.success) return [];
    return data.results.slice(0, 5);
  } catch (err) {
    console.error('Geocode error:', err);
    return [];
  }
}

// Render combined alumni + geocode suggestions
function renderCombinedSuggestions(alumniList, geoList) {
  if (!suggestionsBox) return;

  suggestionsBox.innerHTML = '';

  const hasAlumni = alumniList && alumniList.length > 0;
  const hasGeo = geoList && geoList.length > 0;

  if (!hasAlumni && !hasGeo) {
    suggestionsBox.style.display = 'none';
    return;
  }

  // Alumni suggestions first
  if (hasAlumni) {
    alumniList.forEach((loc) => {
      const div = document.createElement('div');
      div.className = 'suggestion-item';
      div.textContent = `${loc.location} (${loc.count})`;
      div.addEventListener('click', () => {
        searchInput.value = loc.location;
        suggestionsBox.style.display = 'none';
        zoomToLocation(loc);
        if (typeof showLocationDetails === 'function') {
          showLocationDetails(loc);
        }
      });
      suggestionsBox.appendChild(div);
    });
  }

  // Divider between alumni and global results
  if (hasAlumni && hasGeo) {
    const divider = document.createElement('div');
    divider.className = 'suggestion-divider';
    divider.textContent = 'Other locations';
    divider.style.fontSize = '11px';
    divider.style.padding = '4px 8px';
    divider.style.color = '#6b7280';
    divider.style.borderTop = '1px solid #e5e7eb';
    divider.style.marginTop = '4px';
    suggestionsBox.appendChild(divider);
  }

  // Geocode (Nominatim) suggestions
  if (hasGeo) {
    geoList.forEach((item) => {
      const div = document.createElement('div');
      div.className = 'suggestion-item';
      div.textContent = item.display_name;
      div.addEventListener('click', () => {
        searchInput.value = item.display_name;
        suggestionsBox.style.display = 'none';
        zoomToSearchLocation(item);
      });
      suggestionsBox.appendChild(div);
    });
  }

  suggestionsBox.style.display = 'block';
}

// Handle typing in the search box
if (searchInput) {
  searchInput.addEventListener('input', async () => {
    const q = searchInput.value.trim().toLowerCase();
    if (!q) {
      suggestionsBox.style.display = 'none';
      return;
    }

    // Alumni matches from your heatmap data
    const alumniMatches = (locationClusters || [])
      .filter((loc) => (loc.location || '').toLowerCase().includes(q))
      .slice(0, 7);

    // External geocode matches
    const geoMatches = await fetchGeocodeSuggestions(q);

    renderCombinedSuggestions(alumniMatches, geoMatches);
  });

  // Hide suggestions on Escape
  searchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && suggestionsBox) {
      suggestionsBox.style.display = 'none';
    }
  });
}

// Click outside to close dropdown
document.addEventListener('click', (e) => {
  if (!suggestionsBox || !searchInput) return;
  if (!suggestionsBox.contains(e.target) && e.target !== searchInput) {
    suggestionsBox.style.display = 'none';
  }
});

// Search button: prefer alumni match, fallback to geocode
if (searchButton && searchInput) {
  searchButton.addEventListener('click', async () => {
    const q = searchInput.value.trim();
    if (!q) return;
    const lower = q.toLowerCase();

    // 1) Try alumni match
    const alumniMatch = (locationClusters || []).find((loc) =>
      (loc.location || '').toLowerCase().includes(lower)
    );

    if (alumniMatch) {
      zoomToLocation(alumniMatch);
      if (typeof showLocationDetails === 'function') {
        showLocationDetails(alumniMatch);
      }
      return;
    }

    // 2) Fallback to geocode
    const geoResults = await fetchGeocodeSuggestions(q);
    if (geoResults.length > 0) {
      zoomToSearchLocation(geoResults[0]);
    }
  });
}

// =====================================================
// FILTER FUNCTIONALITY
// =====================================================

function populateHeatmapMajorCheckboxes() {
  const container = document.getElementById('heatmapMajorChecks');
  if (!container) return;

  const sortedMajors = HEATMAP_ALLOWED_MAJORS.slice().sort();
  const allowedSet = new Set(sortedMajors);
  selectedHeatmapMajors = new Set(Array.from(selectedHeatmapMajors).filter(m => allowedSet.has(m)));

  container.innerHTML = '';
  sortedMajors.forEach(m => {
    const label = document.createElement('label');
    label.style.display = 'flex';
    label.style.alignItems = 'center';
    label.style.gap = '6px';
    label.style.cursor = 'pointer';
    label.style.fontSize = '0.85rem';
    label.style.padding = '2px 0';
    label.innerHTML = `<input type="checkbox" value="${m}" ${selectedHeatmapMajors.has(m) ? 'checked' : ''} /> ${m}`;
    label.querySelector('input').addEventListener('change', (e) => {
      if (e.target.checked) {
        selectedHeatmapMajors.add(m);
      } else {
        selectedHeatmapMajors.delete(m);
      }
      saveHiddenFiltersToStorage();
      updateFilterBadge();
      reloadMapData();
    });
    container.appendChild(label);
  });
}

function populateHeatmapDegreeCheckboxes() {
  const container = document.getElementById('heatmapDegreeChecks');
  if (!container) return;

  container.innerHTML = '';
  HEATMAP_DEGREE_OPTIONS.forEach(degree => {
    const label = document.createElement('label');
    label.style.display = 'flex';
    label.style.alignItems = 'center';
    label.style.gap = '6px';
    label.style.cursor = 'pointer';
    label.style.fontSize = '0.85rem';
    label.style.padding = '2px 0';
    label.innerHTML = `<input type="checkbox" value="${degree}" ${selectedHeatmapDegrees.has(degree) ? 'checked' : ''} /> ${degree}`;
    label.querySelector('input').addEventListener('change', (e) => {
      if (e.target.checked) {
        selectedHeatmapDegrees.add(degree);
      } else {
        selectedHeatmapDegrees.delete(degree);
      }
      saveHiddenFiltersToStorage();
      updateFilterBadge();
      reloadMapData();
    });
    container.appendChild(label);
  });
}

function populateHeatmapSeniorityCheckboxes() {
  const container = document.getElementById('heatmapSeniorityChecks');
  if (!container) return;

  container.innerHTML = '';
  HEATMAP_SENIORITY_OPTIONS.forEach(level => {
    const label = document.createElement('label');
    label.style.display = 'flex';
    label.style.alignItems = 'center';
    label.style.gap = '6px';
    label.style.cursor = 'pointer';
    label.style.fontSize = '0.85rem';
    label.style.padding = '2px 0';
    label.innerHTML = `<input type="checkbox" value="${level}" ${selectedHeatmapSeniorities.has(level) ? 'checked' : ''} /> ${level}`;
    label.querySelector('input').addEventListener('change', (e) => {
      if (e.target.checked) {
        selectedHeatmapSeniorities.add(level);
      } else {
        selectedHeatmapSeniorities.delete(level);
      }
      saveHiddenFiltersToStorage();
      updateFilterBadge();
      reloadMapData();
    });
    container.appendChild(label);
  });
}

let _filterUIInitialized = false;

function initializeFilterUI() {
  // Refresh major checkboxes on every call (data may have changed)
  populateHeatmapMajorCheckboxes();
  populateHeatmapDegreeCheckboxes();
  populateHeatmapSeniorityCheckboxes();

  if (_filterUIInitialized) return;
  _filterUIInitialized = true;
  const locationInput = document.getElementById('filterLocationInput');
  const companyInput = document.getElementById('filterCompanyInput');
  const untAlumniStatusSelect = document.getElementById('heatmapUntAlumniStatusSelect');
  const addLocationBtn = document.getElementById('addLocationFilterBtn');
  const addCompanyBtn = document.getElementById('addCompanyFilterBtn');
  const clearAllBtn = document.getElementById('clearAllFiltersBtn');
  const filterToggleBtn = document.getElementById('filterToggleBtn');
  const filterPanel = document.getElementById('filterPanel');
  const filterBackdrop = document.getElementById('filterBackdrop');
  const filterCloseBtn = document.getElementById('filterCloseBtn');

  // Load saved filters from localStorage
  loadHiddenFiltersFromStorage();
  if (untAlumniStatusSelect) {
    untAlumniStatusSelect.value = selectedUntAlumniStatus || '';
    untAlumniStatusSelect.addEventListener('change', (e) => {
      selectedUntAlumniStatus = (e.target.value || '').trim().toLowerCase();
      saveHiddenFiltersToStorage();
      updateFilterUI();
      updateFilterBadge();
      reloadMapData();
    });
  }

  // Set up autocomplete suggestions on click/focus
  if (locationInput) {
    locationInput.addEventListener('click', showLocationSuggestions);
    locationInput.addEventListener('focus', showLocationSuggestions);
    locationInput.addEventListener('input', showLocationSuggestions);
  }
  if (companyInput) {
    companyInput.addEventListener('click', showCompanySuggestions);
    companyInput.addEventListener('focus', showCompanySuggestions);
    companyInput.addEventListener('input', showCompanySuggestions);
  }

  const locDropdown = document.getElementById('locationSuggestionsDropdown');
  const comDropdown = document.getElementById('companySuggestionsDropdown');

  locDropdown?.addEventListener('mousedown', (e) => e.preventDefault());
  comDropdown?.addEventListener('mousedown', (e) => e.preventDefault());

  locDropdown?.addEventListener('click', (e) => {
    const item = e.target.closest('.analytics-suggestion-item');
    if (!item) return;
    const location = item.getAttribute('data-value');
    if (!location) return;
    addLocationFilter(location);
    locationInput.value = '';
    locationInput.focus();
    showLocationSuggestions();
  });

  comDropdown?.addEventListener('click', (e) => {
    const item = e.target.closest('.analytics-suggestion-item');
    if (!item) return;
    const company = item.getAttribute('data-value');
    if (!company) return;
    addCompanyFilter(company);
    companyInput.value = '';
    companyInput.focus();
    showCompanySuggestions();
  });

  // Hide suggestions when clicking outside
  document.addEventListener('click', (e) => {
    if (locDropdown && !locDropdown.contains(e.target) && e.target !== locationInput) {
      locDropdown.style.display = 'none';
    }
    if (comDropdown && !comDropdown.contains(e.target) && e.target !== companyInput) {
      comDropdown.style.display = 'none';
    }
  });

  console.log('Initializing Filter UI...');
  console.log('Location Input:', locationInput);
  console.log('Company Input:', companyInput);
  console.log('Add Location Btn:', addLocationBtn);
  console.log('Add Company Btn:', addCompanyBtn);
  console.log('Loaded filters - Locations:', hiddenLocations.size, 'Companies:', hiddenCompanies.size);

  // Toggle filter modal on button click
  if (filterToggleBtn && filterPanel && filterBackdrop) {
    filterToggleBtn.addEventListener('click', () => {
      filterPanel.classList.toggle('active');
      filterBackdrop.classList.toggle('active');
      if (filterPanel.classList.contains('active')) {
        locationInput?.focus();
        document.body.style.overflow = 'hidden'; // Prevent scrolling behind modal
      } else {
        document.body.style.overflow = '';
      }
    });
  }

  // Close filter modal on close button click
  if (filterCloseBtn && filterPanel && filterBackdrop) {
    filterCloseBtn.addEventListener('click', () => {
      filterPanel.classList.remove('active');
      filterBackdrop.classList.remove('active');
      document.body.style.overflow = '';
    });
  }

  // Close filter modal when clicking on backdrop
  if (filterBackdrop && filterPanel) {
    filterBackdrop.addEventListener('click', () => {
      filterPanel.classList.remove('active');
      filterBackdrop.classList.remove('active');
      document.body.style.overflow = '';
    });
  }

  // Close filter modal when clicking ESC key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && filterPanel && filterPanel.classList.contains('active')) {
      filterPanel.classList.remove('active');
      filterBackdrop.classList.remove('active');
      document.body.style.overflow = '';
    }
  });

  if (addLocationBtn) {
    addLocationBtn.addEventListener('click', (e) => {
      e.preventDefault();
      const location = locationInput.value.trim();
      console.log('Add location filter clicked:', location);
      if (location) {
        addLocationFilter(location);
        locationInput.value = '';
        locationInput.focus();
      }
    });
  }

  if (addCompanyBtn) {
    addCompanyBtn.addEventListener('click', (e) => {
      e.preventDefault();
      const company = companyInput.value.trim();
      console.log('Add company filter clicked:', company);
      if (company) {
        addCompanyFilter(company);
        companyInput.value = '';
        companyInput.focus();
      }
    });
  }

  if (clearAllBtn) {
    clearAllBtn.addEventListener('click', () => {
      console.log('Clear all filters clicked');
      hiddenLocations.clear();
      hiddenCompanies.clear();
      selectedUntAlumniStatus = '';
      filterGradYearFrom = null;
      filterGradYearTo   = null;
      selectedHeatmapMajors.clear();
      selectedHeatmapDegrees.clear();
      selectedHeatmapSeniorities.clear();
      if (untAlumniStatusSelect) untAlumniStatusSelect.value = '';
      const gradFromEl = document.getElementById('heatmapGradYearFrom');
      const gradToEl   = document.getElementById('heatmapGradYearTo');
      const gradYearWarning = document.getElementById('heatmapGradYearWarning');
      if (gradFromEl) gradFromEl.value = '';
      if (gradToEl)   gradToEl.value   = '';
      setInputWarning(gradYearWarning, '');
      document.querySelectorAll('#heatmapMajorChecks input[type="checkbox"]').forEach(cb => cb.checked = false);
      document.querySelectorAll('#heatmapDegreeChecks input[type="checkbox"]').forEach(cb => cb.checked = false);
      document.querySelectorAll('#heatmapSeniorityChecks input[type="checkbox"]').forEach(cb => cb.checked = false);
      clearHiddenFiltersFromStorage();
      updateFilterUI();
      updateFilterBadge();
      reloadMapData();
    });
  }

  // Wire up graduation year Apply button
  const applyGradYearBtn = document.getElementById('applyGradYearFilterBtn');
  const gradFromEl = document.getElementById('heatmapGradYearFrom');
  const gradToEl   = document.getElementById('heatmapGradYearTo');
  const gradYearWarning = document.getElementById('heatmapGradYearWarning');

  enforceIntegerOnlyInput(gradFromEl, gradYearWarning);
  enforceIntegerOnlyInput(gradToEl, gradYearWarning);

  // Pre-populate inputs from URL params or localStorage
  if (gradFromEl && filterGradYearFrom != null) gradFromEl.value = String(filterGradYearFrom);
  if (gradToEl   && filterGradYearTo   != null) gradToEl.value   = String(filterGradYearTo);

  if (applyGradYearBtn) {
    applyGradYearBtn.addEventListener('click', () => {
      const parsed = parseValidatedIntegerRange(gradFromEl, gradToEl, gradYearWarning);
      if (!parsed.valid) return;
      filterGradYearFrom = parsed.min;
      filterGradYearTo = parsed.max;
      saveHiddenFiltersToStorage();
      updateFilterUI();
      updateFilterBadge();
      reloadMapData();
    });
  }

  // Allow Enter key to add filters
  if (locationInput) {
    locationInput.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        addLocationBtn.click();
      }
    });
  }

  if (companyInput) {
    companyInput.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        addCompanyBtn.click();
      }
    });
  }

  updateFilterUI();
  updateFilterBadge();

  // Apply saved filters if any were loaded
  if (hiddenLocations.size > 0 || hiddenCompanies.size > 0 || selectedUntAlumniStatus || selectedHeatmapMajors.size > 0 || selectedHeatmapDegrees.size > 0 || selectedHeatmapSeniorities.size > 0 || filterGradYearFrom != null || filterGradYearTo != null) {
    console.log('Applying saved filters from localStorage');
    reloadMapData();
  }
}

function addLocationFilter(location) {
  const normalizedLocation = location.trim().toLowerCase();
  hiddenLocations.add(normalizedLocation);
  console.log('Location filter added:', normalizedLocation);
  console.log('Hidden Locations:', hiddenLocations);
  console.log('Location Clusters:', locationClusters);
  updateFilterUI();
  updateFilterBadge();
  saveHiddenFiltersToStorage();
  reloadMapData();
}

function addCompanyFilter(company) {
  const normalizedCompany = company.trim().toLowerCase();
  hiddenCompanies.add(normalizedCompany);
  console.log('Company filter added:', normalizedCompany);
  console.log('Hidden Companies:', hiddenCompanies);
  console.log('Location Clusters:', locationClusters);
  updateFilterUI();
  updateFilterBadge();
  saveHiddenFiltersToStorage();
  reloadMapData();
}

function removeLocationFilter(location) {
  const normalizedLocation = location.trim().toLowerCase();
  hiddenLocations.delete(normalizedLocation);
  saveHiddenFiltersToStorage();
  updateFilterUI();
  updateFilterBadge();
  reloadMapData();
}

function removeCompanyFilter(company) {
  const normalizedCompany = company.trim().toLowerCase();
  hiddenCompanies.delete(normalizedCompany);
  saveHiddenFiltersToStorage();
  updateFilterUI();
  updateFilterBadge();
  reloadMapData();
}

function updateFilterBadge() {
  const badge = document.getElementById('filterBadge');
  if (badge) {
    const gradYearActive = (filterGradYearFrom != null || filterGradYearTo != null) ? 1 : 0;
    const totalFilters = hiddenLocations.size + hiddenCompanies.size + (selectedUntAlumniStatus ? 1 : 0) + gradYearActive + selectedHeatmapMajors.size + selectedHeatmapDegrees.size + selectedHeatmapSeniorities.size;
    badge.textContent = totalFilters;
    badge.style.display = totalFilters > 0 ? 'flex' : 'none';
  }
}

function updateFilterUI() {
  const gradYearActive = (filterGradYearFrom != null || filterGradYearTo != null) ? 1 : 0;
  const totalFilters = hiddenLocations.size + hiddenCompanies.size + (selectedUntAlumniStatus ? 1 : 0) + gradYearActive + selectedHeatmapMajors.size + selectedHeatmapDegrees.size + selectedHeatmapSeniorities.size;
  const clearAllBtn = document.getElementById('clearAllFiltersBtn');
  if (clearAllBtn) {
    clearAllBtn.disabled = totalFilters === 0;
  }

  // Update location filter tags
  const locationTags = document.getElementById('locationFilterTags');
  const locationCount = document.getElementById('locationFilterCount');

  if (locationTags) {
    locationTags.innerHTML = hiddenLocations.size === 0
      ? '<span class="empty-analytics-filters-message">No locations hidden</span>'
      : Array.from(hiddenLocations)
          .map((location) => `
            <span class="analytics-filter-tag">
              <span>${escapeHtml(location)}</span>
              <button class="analytics-filter-tag-remove" onclick="removeLocationFilter('${location.replace(/'/g, "\\'")}')">×</button>
            </span>
          `)
          .join('');
  }
  if (locationCount) {
    locationCount.textContent = hiddenLocations.size;
  }

  // Update company filter tags
  const companyTags = document.getElementById('companyFilterTags');
  const companyCount = document.getElementById('companyFilterCount');

  if (companyTags) {
    companyTags.innerHTML = hiddenCompanies.size === 0
      ? '<span class="empty-analytics-filters-message">No companies hidden</span>'
      : Array.from(hiddenCompanies)
          .map((company) => `
            <span class="analytics-filter-tag">
              <span>${escapeHtml(company)}</span>
              <button class="analytics-filter-tag-remove" onclick="removeCompanyFilter('${company.replace(/'/g, "\\'")}')">×</button>
            </span>
          `)
          .join('');
  }
  if (companyCount) {
    companyCount.textContent = hiddenCompanies.size;
  }

  const untAlumniStatusTag = document.getElementById('heatmapUntAlumniStatusTag');
  if (untAlumniStatusTag) {
    if (!selectedUntAlumniStatus) {
      untAlumniStatusTag.innerHTML = '<span class="empty-analytics-filters-message">All statuses</span>';
    } else {
      const label = selectedUntAlumniStatus.charAt(0).toUpperCase() + selectedUntAlumniStatus.slice(1);
      untAlumniStatusTag.innerHTML = `
        <span class="analytics-filter-tag">
          <span>${escapeHtml(label)}</span>
          <button class="analytics-filter-tag-remove" onclick="clearHeatmapUntAlumniStatusFilter()">×</button>
        </span>
      `;
    }
  }

  // Graduation year tag
  const gradYearTag = document.getElementById('heatmapGradYearTag');
  const seniorityTag = document.getElementById('heatmapSeniorityTag');
  const degreeTag = document.getElementById('heatmapDegreeTag');
  if (gradYearTag) {
    if (filterGradYearFrom == null && filterGradYearTo == null) {
      gradYearTag.innerHTML = '<span class="empty-analytics-filters-message">All years</span>';
    } else {
      let rangeLabel;
      if (filterGradYearFrom != null && filterGradYearTo != null) rangeLabel = `${filterGradYearFrom} – ${filterGradYearTo}`;
      else if (filterGradYearFrom != null)                        rangeLabel = `${filterGradYearFrom} and later`;
      else                                                        rangeLabel = `up to ${filterGradYearTo}`;
      gradYearTag.innerHTML = `
        <span class="analytics-filter-tag">
          <span>${escapeHtml(rangeLabel)}</span>
          <button class="analytics-filter-tag-remove" onclick="clearGradYearInlineFilter()">×</button>
        </span>
      `;
    }
  }

  if (seniorityTag) {
    if (selectedHeatmapSeniorities.size === 0) {
      seniorityTag.innerHTML = '<span class="empty-analytics-filters-message">All levels</span>';
    } else {
      seniorityTag.innerHTML = Array.from(selectedHeatmapSeniorities)
        .map((level) => `
          <span class="analytics-filter-tag">
            <span>${escapeHtml(level)}</span>
            <button class="analytics-filter-tag-remove" onclick="removeHeatmapSeniorityFilter('${level.replace(/'/g, "\\'")}')">×</button>
          </span>
        `)
        .join('');
    }
  }

  if (degreeTag) {
    if (selectedHeatmapDegrees.size === 0) {
      degreeTag.innerHTML = '<span class="empty-analytics-filters-message">All degrees</span>';
    } else {
      degreeTag.innerHTML = Array.from(selectedHeatmapDegrees)
        .map((level) => `
          <span class="analytics-filter-tag">
            <span>${escapeHtml(level)}</span>
            <button class="analytics-filter-tag-remove" onclick="removeHeatmapDegreeFilter('${level.replace(/'/g, "\\'")}')">×</button>
          </span>
        `)
        .join('');
    }
  }
}

function clearHeatmapUntAlumniStatusFilter() {
  selectedUntAlumniStatus = '';
  const untAlumniStatusSelect = document.getElementById('heatmapUntAlumniStatusSelect');
  if (untAlumniStatusSelect) untAlumniStatusSelect.value = '';
  saveHiddenFiltersToStorage();
  updateFilterUI();
  updateFilterBadge();
  reloadMapData();
}

function clearGradYearInlineFilter() {
  filterGradYearFrom = null;
  filterGradYearTo   = null;
  const gradFromEl = document.getElementById('heatmapGradYearFrom');
  const gradToEl   = document.getElementById('heatmapGradYearTo');
  if (gradFromEl) gradFromEl.value = '';
  if (gradToEl)   gradToEl.value   = '';
  const gradYearWarning = document.getElementById('heatmapGradYearWarning');
  setInputWarning(gradYearWarning, '');
  saveHiddenFiltersToStorage();
  updateFilterUI();
  updateFilterBadge();
  reloadMapData();
}

function removeHeatmapSeniorityFilter(level) {
  selectedHeatmapSeniorities.delete(level);
  const input = document.querySelector(`#heatmapSeniorityChecks input[value="${CSS.escape(level)}"]`);
  if (input) input.checked = false;
  saveHiddenFiltersToStorage();
  updateFilterUI();
  updateFilterBadge();
  reloadMapData();
}

function removeHeatmapDegreeFilter(level) {
  selectedHeatmapDegrees.delete(level);
  const input = document.querySelector(`#heatmapDegreeChecks input[value="${CSS.escape(level)}"]`);
  if (input) input.checked = false;
  saveHiddenFiltersToStorage();
  updateFilterUI();
  updateFilterBadge();
  reloadMapData();
}

function isLocationHidden(location) {
  if (!location) return false;
  const locationName = location.trim().toLowerCase();

  // Check if any hidden location filter matches this location (substring match)
  let isHidden = false;
  for (let hiddenLocation of hiddenLocations) {
    if (locationName.includes(hiddenLocation)) {
      isHidden = true;
      break;
    }
  }

  console.log(`Checking location "${location}" (normalized: "${locationName}"): hidden = ${isHidden}`);
  return isHidden;
}

function isCompanyHidden(alumni) {
  if (!alumni || !alumni.company) return false;
  const companyName = alumni.company.trim().toLowerCase();

  // Check if any hidden company filter matches this company (substring match)
  let isHidden = false;
  for (let hiddenCompany of hiddenCompanies) {
    if (companyName.includes(hiddenCompany) || hiddenCompany.includes(companyName)) {
      isHidden = true;
      break;
    }
  }

  console.log(`Checking company "${alumni.company}" (normalized: "${companyName}"): hidden = ${isHidden}`);
  return isHidden;
}

function matchesUntAlumniStatus(alumni) {
  if (!selectedUntAlumniStatus) return true;
  const status = (alumni?.unt_alumni_status || 'unknown').toLowerCase();
  return status === selectedUntAlumniStatus;
}

function shouldHideLocation(location) {
  return isLocationHidden(location.location);
}

function shouldHideLocationAlumni(location) {
  // Hide if location is hidden OR if all alumni in the location are from hidden companies
  if (isLocationHidden(location.location)) {
    console.log(`Location "${location.location}" is hidden (exact match)`);
    return true;
  }

  // Check if all alumni in this location are from hidden companies
  console.log(`Checking alumni for location "${location.location}":`, location.sample_alumni);

  const hasVisibleAlumni = location.sample_alumni.some(a => {
    const isHidden = isCompanyHidden(a);
    console.log(`  - Alumni: ${a.name}, Company: "${a.company}" (trimmed: "${(a.company || '').trim().toLowerCase()}"), Hidden: ${isHidden}`);
    return !isHidden;
  });

  const shouldHide = !hasVisibleAlumni;
  console.log(`Location "${location.location}": has visible alumni = ${hasVisibleAlumni}, shouldHide = ${shouldHide}`);
  return shouldHide;
}

function getVisibleAlumniForLocation(location) {
  const alumni = Array.isArray(location?.sample_alumni) ? location.sample_alumni : [];
  return alumni.filter(a => !isCompanyHidden(a) && matchesUntAlumniStatus(a));
}

/** Build the /api/heatmap URL incorporating all currently-active filter params. */
function buildHeatmapUrl() {
  const params = new URLSearchParams();
  if (filterGradYearFrom != null) params.set('grad_year_from', String(filterGradYearFrom));
  if (filterGradYearTo   != null) params.set('grad_year_to',   String(filterGradYearTo));
  if (selectedUntAlumniStatus) params.set('unt_alumni_status', selectedUntAlumniStatus);
  selectedHeatmapMajors.forEach(m => params.append('standardized_major', m));
  selectedHeatmapDegrees.forEach(d => params.append('degree', d));
  selectedHeatmapSeniorities.forEach(s => params.append('seniority', s));
  return params.toString() ? `/api/heatmap?${params}` : '/api/heatmap';
}

function reloadMapData() {
  console.log('=== Reloading Map Data ===');
  console.log('Hidden Locations:', Array.from(hiddenLocations));
  console.log('Hidden Companies:', Array.from(hiddenCompanies));

  // If any server-side filters are active, re-fetch from the API so the
  // dataset is correctly filtered at the DB level (grad year, unt status)
  const hasServerFiltersActive = (filterGradYearFrom != null || filterGradYearTo != null || selectedUntAlumniStatus || selectedHeatmapMajors.size > 0 || selectedHeatmapDegrees.size > 0 || selectedHeatmapSeniorities.size > 0);
  if (hasServerFiltersActive || currentDatasetServerFiltered) {
    const targetUrl = hasServerFiltersActive ? buildHeatmapUrl() : '/api/heatmap';
    loadHeatmapData(targetUrl);
    return;
  }

  try {
    // Clear all old markers and entities
    console.log('Clearing old markers...');
    markers2D.forEach(m => {
      try {
        map2D.removeLayer(m);
      } catch (e) {
        console.error('Error removing marker:', e);
      }
    });
    markers2D = [];

    if (heatLayer2D) {
      try {
        map2D.removeLayer(heatLayer2D);
      } catch (e) {
        console.error('Error removing heat layer:', e);
      }
      heatLayer2D = null;
    }

    entities3D.forEach(e => {
      if (window.map3DAvailable && map3D) {
        try {
          map3D.entities.remove(e);
        } catch (e) {
          console.error('Error removing 3D entity:', e);
        }
      }
    });
    entities3D = [];

    // Filter and re-add markers
    console.log('Filtering locations...');
    const filteredLocations = locationClusters
      .map(loc => {
        if (isLocationHidden(loc.location)) {
          console.log(`Location: ${loc.location}, Hide: true (location filter), Alumni count: ${loc.count}`);
          return null;
        }

        const visibleAlumni = getVisibleAlumniForLocation(loc);
        const visibleCount = visibleAlumni.length;
        const shouldHide = visibleCount === 0;
        console.log(`Location: ${loc.location}, Hide: ${shouldHide}, Alumni count: ${loc.count}, Visible: ${visibleCount}`);
        if (shouldHide) {
          return null;
        }

        // IMPORTANT: use visible alumni/count for rendering and stats.
        return {
          ...loc,
          sample_alumni: visibleAlumni,
          count: visibleCount
        };
      })
      .filter(Boolean);

    console.log('Filtered locations count:', filteredLocations.length);

    if (filteredLocations.length === 0) {
      console.warn('No locations to display after filtering!');
    }

    filteredLocations.forEach(location => {
      try {
        add2DMarker(location);
        if (window.map3DAvailable && map3D) {
          add3DMarker(location);
        }
      } catch (e) {
        console.error('Error adding marker:', e);
      }
    });

    // Rebuild heatmap
    console.log('Rebuilding heatmap...');
    const maxCount = filteredLocations.length > 0
      ? Math.max(...filteredLocations.map(l => l.count || 1))
      : 1;
    render2DHeatmap(filteredLocations, maxCount);

    // Update statistics with filtered data
    const totalFiltered = filteredLocations.reduce((sum, loc) => sum + (loc.count || 0), 0);
    updateStatistics(totalFiltered, filteredLocations.length, {
      totalFilteredAlumni: totalFiltered,
      missingLocationCount: 0,
      ungeocodedWithLocationCount: 0,
    });

    console.log('Map reload complete!');
  } catch (error) {
    console.error('ERROR in reloadMapData:', error);
    console.error(error.stack);
  }
}
