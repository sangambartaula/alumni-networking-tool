// heatmap.js
// Leaflet-based heatmap for alumni location visualization

let map;
let heatLayer;
let locationClusters = [];
let pieChart = null;

// Lat/Lon bounding boxes for each continent
const CONTINENT_BOUNDS = {
  "North America": [[10, -170], [83, -50]],
  "South America": [[-56, -81], [13, -34]],
  "Europe": [[35, -25], [71, 45]],
  "Asia": [[1, 26], [77, 180]],
  "Africa": [[-35, -20], [38, 52]],
  "Oceania": [[-50, 110], [0, 180]],
  "Antarctica": [[-90, -180], [-60, 180]]
};

// Color mapping for continents
const CONTINENT_COLORS = {
  "North America": "#667eea",
  "South America": "#764ba2",
  "Europe": "#f093fb",
  "Asia": "#4facfe",
  "Africa": "#ffa400",
  "Oceania": "#43e97b",
  "Antarctica": "#fa709a"
};

// Initialize map on page load
document.addEventListener('DOMContentLoaded', () => {
  initializeMap();
  loadHeatmapData();
  setupFullscreenToggle();
  setupAutoHideHeader();
});

// Auto-hide header on mouse movement
function setupAutoHideHeader() {
  const header = document.getElementById('heatmapHeader');
  const heatmapSection = document.querySelector('.heatmap-section');
  let hideTimeout;
  let isHeaderVisible = true;
  
  // Show header initially for 3 seconds, then hide
  setTimeout(() => {
    header.classList.add('hidden');
    isHeaderVisible = false;
  }, 3000);
  
  // Track mouse movement
  heatmapSection.addEventListener('mousemove', (e) => {
    const mouseY = e.clientY;
    
    // Only show header when mouse is very close to the top (within 30px)
    if (mouseY < 30) {
      if (!isHeaderVisible) {
        header.classList.remove('hidden');
        isHeaderVisible = true;
      }
      
      // Clear any existing timeout
      clearTimeout(hideTimeout);
    } else if (mouseY > 100 && isHeaderVisible) {
      // Hide header when mouse moves away from top area
      clearTimeout(hideTimeout);
      hideTimeout = setTimeout(() => {
        header.classList.add('hidden');
        isHeaderVisible = false;
      }, 800);
    }
  });
  
  // Keep header visible when mouse is over it
  header.addEventListener('mouseenter', () => {
    clearTimeout(hideTimeout);
    header.classList.remove('hidden');
    isHeaderVisible = true;
  });
  
  // Hide header when mouse leaves it and is not near the top
  header.addEventListener('mouseleave', (e) => {
    if (e.clientY > 100) {
      hideTimeout = setTimeout(() => {
        header.classList.add('hidden');
        isHeaderVisible = false;
      }, 1500);
    }
  });
}

// Fullscreen toggle functionality
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
    
    // Invalidate map size after transition
    setTimeout(() => {
      map.invalidateSize();
    }, 300);
  });
  
  // ESC key to exit fullscreen
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && heatmapSection.classList.contains('fullscreen-mode')) {
      fullscreenBtn.click();
    }
  });
}

function initializeMap() {
  // Create map centered on USA
  map = L.map('heatmapContainer').setView([39.8283, -98.5795], 4);
  
  // Define base layers (different map styles)
  
  // Google Maps - Default road map (like when you open Google Maps)
  const googleMaps = L.tileLayer('https://mt1.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {
    attribution: '&copy; Google Maps',
    maxZoom: 20,
    subdomains: ['mt0', 'mt1', 'mt2', 'mt3']
  });
  
  // Google Earth Hybrid - Satellite with roads and labels (like Google Earth)
  const googleEarthHybrid = L.tileLayer('https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
    attribution: '&copy; Google Earth',
    maxZoom: 20,
    subdomains: ['mt0', 'mt1', 'mt2', 'mt3']
  });
  
  // Google Terrain - Shows elevation and terrain features
  const googleTerrain = L.tileLayer('https://mt1.google.com/vt/lyrs=p&x={x}&y={y}&z={z}', {
    attribution: '&copy; Google',
    maxZoom: 20,
    subdomains: ['mt0', 'mt1', 'mt2', 'mt3']
  });
  
  // Esri Satellite + Labels (backup option)
  const hybridImagery = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
    attribution: 'Tiles &copy; Esri',
    maxZoom: 19
  });
  
  const transportationLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Transportation/MapServer/tile/{z}/{y}/{x}', {
    attribution: '',
    maxZoom: 19
  });
  
  const hybridLabels = L.tileLayer('https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}', {
    attribution: '',
    maxZoom: 19
  });
  
  const esriHybrid = L.layerGroup([hybridImagery, transportationLayer, hybridLabels]);
  
  // Add default layer (Google Earth View)
  googleEarthHybrid.addTo(map);
  
  // Create layer control object
  const baseLayers = {
    "<span style='font-weight: 500;'>🌍 Google Earth View (Recommended)</span>": googleEarthHybrid,
    "<span style='font-weight: 500;'>🗺️ Google Maps</span>": googleMaps,
    "<span style='font-weight: 500;'>⛰️ Google Terrain</span>": googleTerrain,
    "<span style='font-weight: 500;'>🛰️ Satellite + Labels (Esri)</span>": esriHybrid
  };
  
  // Add layer control to map (bottom-left corner, away from header)
  L.control.layers(baseLayers, null, {
    position: 'bottomleft',
    collapsed: false
  }).addTo(map);
}

function highlightButton(continent) {
  const buttons = document.querySelectorAll(".continent-btn");
  buttons.forEach(btn => btn.classList.remove("selected"));

  if (!continent) return;

  const selected = document.querySelector(`.continent-btn[data-continent="${continent}"]`);
  if (selected) {
    selected.classList.add("selected");
  }
}


function filterByContinent(continent) {
  // Hide sidebar when switching continents
  highlightButton(continent);
  //document.getElementById('heatmapSidebar').style.display = 'none';
  document.getElementById('heatmapSidebar').classList.remove('visible');
  const url = `/api/heatmap?continent=${encodeURIComponent(continent)}`;
  loadHeatmapData(url);

  if (continent === "North America") {
    // Center around US/Mexico/Canada
    map.setView([40, -100], 3);
  } else if (continent === "Oceania") {
    // Center around Australia/NZ
    map.setView([-25, 135], 4);
  } else if (CONTINENT_BOUNDS[continent]) {
    // Use bounds for the rest
    map.fitBounds(CONTINENT_BOUNDS[continent]);
  }
}

function resetContinent() {
  highlightButton(null);
  //document.getElementById('heatmapSidebar').style.display = 'none';
  document.getElementById('heatmapSidebar').classList.remove('visible');
  // Reset map view to full US-centered view
  map.setView([39.8283, -98.5795], 4);
  loadHeatmapData('/api/heatmap');
}


async function loadHeatmapData(url = '/api/heatmap') {
  try {
    const response = await fetch('/api/heatmap');
    const data = await response.json();
    
    if (!data.success) {
      showError('Failed to load heatmap data');
      return;
    }
    
    locationClusters = data.locations;
    
    if (!locationClusters || locationClusters.length === 0) {
      showError('No alumni with coordinates found. Geocoding may still be in progress.');
      return;
    }
    
    // Prepare heatmap intensity data (weight by alumni count)
    const heatmapData = locationClusters.map(loc => [
      loc.latitude,
      loc.longitude,
      Math.min(1.0, loc.count / 10) // Normalize intensity (0-1 scale, capped at count=10)
    ]);
    
    // Add heat layer to map (use Leaflet Heat if available, otherwise fall back)
    if (heatLayer) {
      map.removeLayer(heatLayer);
    }

    if (typeof L.heatLayer === 'function') {
      heatLayer = L.heatLayer(heatmapData, {
        radius: 25,
        blur: 15,
        maxZoom: 17,
        minOpacity: 0.3,
        gradient: {
          0.0: '#0000ff',  // Blue
          0.25: '#00ff00', // Green
          0.5: '#ffff00',  // Yellow
          0.75: '#ff8800',  // Orange
          1.0: '#ff0000'   // Red
        }
      }).addTo(map);
    } else {
      // Fallback: create a layer group of semi-transparent circles to approximate a heatmap
      console.warn('Leaflet heat plugin not available; using circle-marker fallback.');
      const fallbackLayer = L.layerGroup();
      heatmapData.forEach(([lat, lon, intensity]) => {
        // intensity is normalized 0..1; scale radius and opacity accordingly
        const countEstimate = Math.round(intensity * 10);
        const radiusMeters = 2000 + intensity * 12000; // meters (visual scale)
        const circle = L.circle([lat, lon], {
          radius: radiusMeters,
          fillColor: getColorForCount(countEstimate),
          color: '#333',
          weight: 1,
          opacity: 0.6,
          fillOpacity: 0.15 + intensity * 0.6
        });
        fallbackLayer.addLayer(circle);
      });
      heatLayer = fallbackLayer.addTo(map);
    }
    
    // Add clickable markers for each location cluster
    locationClusters.forEach(location => {
      const marker = L.circleMarker([location.latitude, location.longitude], {
        radius: 8 + Math.sqrt(location.count) * 2,
        fillColor: getColorForCount(location.count),
        color: '#333',
        weight: 2,
        opacity: 0.8,
        fillOpacity: 0.6
      }).addTo(map);
      
      // Popup with location info
      const popupContent = createLocationPopup(location);
      //marker.bindPopup(popupContent);

      // Permanent count badge shown on the map (centered on the marker)
      // Use a tooltip with a custom class so we can style it as a badge
      try {
        marker.bindTooltip(`${location.count}`, {
          permanent: true,
          direction: 'center',
          className: 'count-badge'
        }).openTooltip();
      } catch (e) {
        // In case bindTooltip fails for some versions, silently continue
        console.warn('Could not bind tooltip for count badge', e);
      }
      
      // Click event to show details in sidebar
      marker.on('click', () => {
        showLocationDetails(location);
      });
    });
    
    // Update statistics
    updateStatistics(data.total_alumni, locationClusters.length);
    
    // Update pie chart
    updatePieChart(locationClusters);
    
  } catch (error) {
    console.error('Error loading heatmap data:', error);
    showError('Error loading heatmap data: ' + error.message);
  }
}

function getColorForCount(count) {
  // Color intensity based on alumni count
  if (count >= 10) return '#ff0000';  // Red
  if (count >= 7) return '#ff8800';   // Orange
  if (count >= 5) return '#ffff00';   // Yellow
  if (count >= 3) return '#00ff00';   // Green
  return '#0000ff';                    // Blue
}

function createLocationPopup(location) {
  return `
    <div class="popup-content">
      <h4>${location.location}</h4>
      <p><strong>Alumni Count:</strong> ${location.count}</p>
      <p class="popup-hint">Click this marker to view detailed alumni info in the side panel.</p>
    </div>
  `;
}


function showLocationDetails(location) {
  const sidebarContainer = document.getElementById('heatmapSidebar');
  const sidebar = document.getElementById('locationDetails');

  if (sidebarContainer) {
    sidebarContainer.classList.add('visible');
  }
  
  
    const alumniItems = location.sample_alumni
    .map(a => `
      <div class="location-alumni-item">
      <div class="alumni-info">
        <div class="alumni-name">${a.name}</div>
        <div class="alumni-role">${a.role}</div>
        ${a.company ? `<div class="alumni-company">${a.company}</div>` : ''}
      </div>

      ${a.linkedin ? `
        <a class="linkedin-icon-btn" href="${a.linkedin}" target="_blank" rel="noopener noreferrer">
          <img src="linkedin.svg" alt="LinkedIn" />
        </a>
      ` : ''}
    </div>
  `)
  .join('');

  sidebar.innerHTML = `
    <div class="location-details-card">
      <h4>${location.location}</h4>
      <div class="location-meta">
        <span class="badge badge-primary">${location.count} Alumni</span>
        <span class="badge badge-secondary">Lat: ${location.latitude.toFixed(3)}</span>
        <span class="badge badge-secondary">Lon: ${location.longitude.toFixed(3)}</span>
      </div>
      <h5>Alumni (${location.count})</h5>
      <div class="alumni-list">
        ${alumniItems}
      </div>
    </div>
  `;
}

function updateStatistics(totalAlumni, uniqueLocations) {
  const statsDiv = document.getElementById('heatmapStats');
  statsDiv.innerHTML = `
    <div class="stat-item">
      <span class="stat-label">Total Alumni:</span>
      <span class="stat-value">${totalAlumni}</span>
    </div>
    <div class="stat-item">
      <span class="stat-label">Unique Locations:</span>
      <span class="stat-value">${uniqueLocations}</span>
    </div>
  `;
}

function showError(message) {
  const sidebar = document.getElementById('locationDetails');
  sidebar.innerHTML = `<p style="color: #e74c3c;">${message}</p>`;
}
// Close sidebar when clicking outside it
document.addEventListener('click', function (event) {
  const sidebar = document.getElementById('heatmapSidebar');
  const mapContainer = document.getElementById('heatmapContainer');

  // If sidebar is hidden, do nothing
  if (!sidebar || sidebar.style.display === 'none') return;

  // If click is inside sidebar, do nothing
  if (sidebar.contains(event.target)) return;

  // If click is a marker, do NOT close sidebar (marker click will update it)
  if (event.target.closest('.leaflet-marker-icon') || 
      event.target.closest('.leaflet-interactive')) {
    return;
  }

  // If click is anywhere else on the page  then close sidebar
  sidebar.classList.remove('visible');
});

function aggregateAlumniByContinent(locations) {
  // Aggregate alumni counts by continent based on location coordinates
  const continentCounts = {};
  
  // Initialize all continents with 0
  Object.keys(CONTINENT_BOUNDS).forEach(continent => {
    continentCounts[continent] = 0;
  });
  
  locations.forEach(location => {
    const lat = location.latitude;
    const lon = location.longitude;
    
    // Determine which continent this location belongs to
    for (const [continent, [[minLat, minLon], [maxLat, maxLon]]] of Object.entries(CONTINENT_BOUNDS)) {
      if (lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon) {
        continentCounts[continent] += location.count;
        break;
      }
    }
  });
  
  return continentCounts;
}

function updatePieChart(locations) {
  const continentCounts = aggregateAlumniByContinent(locations);
  
  // Filter out continents with 0 alumni
  const labels = Object.keys(continentCounts).filter(continent => continentCounts[continent] > 0);
  const data = labels.map(continent => continentCounts[continent]);
  const colors = labels.map(continent => CONTINENT_COLORS[continent] || "#999");
  
  const chartCanvas = document.getElementById('continentPieChart');
  
  if (!chartCanvas) {
    console.warn('Pie chart canvas element not found');
    return;
  }
  
  // Destroy existing chart if it exists
  if (pieChart) {
    pieChart.destroy();
  }
  
  // Create new pie chart
  const ctx = chartCanvas.getContext('2d');
  pieChart = new Chart(ctx, {
    type: 'pie',
    data: {
      labels: labels,
      datasets: [{
        data: data,
        backgroundColor: colors,
        borderColor: '#fff',
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: {
          position: 'bottom',
          labels: {
            padding: 15,
            font: {
              size: 13
            }
          }
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              const total = context.dataset.data.reduce((a, b) => a + b, 0);
              const percentage = ((context.parsed / total) * 100).toFixed(1);
              return `${context.label}: ${context.parsed} (${percentage}%)`;
            }
          }
        }
      }
    }
  });
}
