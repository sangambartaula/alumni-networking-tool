// heatmap.js
// Leaflet-based heatmap for alumni location visualization

let map;
let heatLayer;
let locationClusters = [];

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

async function loadHeatmapData() {
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
    
    // Add heat layer to map
    if (heatLayer) {
      map.removeLayer(heatLayer);
    }
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
      marker.bindPopup(popupContent);
      
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
  const alumniList = location.sample_alumni
    .map(a => `<li><strong>${a.name}</strong><br/>${a.role}${a.company ? '<br/>@ ' + a.company : ''}</li>`)
    .join('');
  
  return `
    <div class="popup-content">
      <h4>${location.location}</h4>
      <p><strong>Alumni Count:</strong> ${location.count}</p>
      <p><strong>Sample Alumni:</strong></p>
      <ul style="margin: 0; padding-left: 20px;">
        ${alumniList}
      </ul>
    </div>
  `;
}

function showLocationDetails(location) {
  const sidebar = document.getElementById('locationDetails');
  
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
      <h5>Sample Alumni (${Math.min(3, location.sample_alumni.length)})</h5>
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
