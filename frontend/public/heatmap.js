// heatmap.js
// Leaflet-based heatmap for alumni location visualization

let map;
let heatLayer;
let locationClusters = [];

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

// Initialize map on page load
document.addEventListener('DOMContentLoaded', () => {
  initializeMap();
  loadHeatmapData();
});

function initializeMap() {
  // Create map centered on USA
  map = L.map('heatmapContainer').setView([39.8283, -98.5795], 4);
  
  // Add OpenStreetMap tiles
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19
  }).addTo(map);
}

function filterByContinent(continent) {
  // Hide sidebar when switching continents
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
        <div class="alumni-name">${a.name}</div>
        <div class="alumni-role">${a.role}</div>
        ${a.company ? `<div class="alumni-company">${a.company}</div>` : ''}
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
