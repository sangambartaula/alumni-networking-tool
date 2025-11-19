// heatmap_dual.js - Dual map system with both 2D Leaflet and 3D Cesium

// Set Cesium Ion access token
Cesium.Ion.defaultAccessToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJlYWE1OWUxNy1mMWZiLTQzYjYtYTQ0OS1kMWFjYmFkNjc5YzciLCJpZCI6NTc3MzMsImlhdCI6MTYyNzg0NTE4Mn0.XcKpgANiY19MC4bdFUXMVEBToBmqS8kuYpUlxJHYZxk';

let map2D;
let map3D;
let locationClusters = [];
let pieChart = null;
let currentMode = '2d'; // Start with 2D
let markers2D = [];
let entities3D = [];

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
  initialize2DMap();
  initialize3DMap();
  loadHeatmapData();
  setupFullscreenToggle();
  setupHeaderToggle();
  add2D3DToggle();
  addLayerControls();
});

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
    fullscreenButton: true,
    geocoder: false,
    homeButton: true,
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
  map3D.scene.moon.show = false;
  map3D.scene.sun.show = true;
  map3D.scene.globe.show = true;
  map3D.scene.globe.baseColor = Cesium.Color.WHITE;
  map3D.scene.requestRenderMode = false;
  map3D.scene.highDynamicRange = true;
  
  // Set fixed camera view focused on USA
  map3D.camera.setView({
    destination: Cesium.Cartesian3.fromDegrees(-98.5795, 39.8283, 8000000),
    orientation: {
      heading: 0.0,
      pitch: Cesium.Math.toRadians(-45),
      roll: 0.0
    }
  });
  
  // Enable camera controls so users can navigate
  map3D.scene.screenSpaceCameraController.enableRotate = true;
  map3D.scene.screenSpaceCameraController.enableZoom = true;
  map3D.scene.screenSpaceCameraController.enableTilt = true;
  map3D.scene.screenSpaceCameraController.enableLook = true;
  
  // Set minimum and maximum zoom distances
  map3D.scene.screenSpaceCameraController.minimumZoomDistance = 1000; // Can't zoom closer than 1km
  map3D.scene.screenSpaceCameraController.maximumZoomDistance = 20000000; // Can't zoom out past 20,000km
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
  buttons.forEach(btn => {
    btn.addEventListener('click', () => {
      const mode = btn.dataset.mode;
      
      // Update active state
      buttons.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      
      switchMapMode(mode);
    });
  });
}

// Switch between 2D and 3D maps
function switchMapMode(mode) {
  currentMode = mode;
  
  const map2DContainer = document.getElementById('map2DContainer');
  const map3DContainer = document.getElementById('map3DContainer');
  
  if (mode === '2d') {
    map2DContainer.style.display = 'block';
    map3DContainer.style.display = 'none';
    
    // Refresh Leaflet map
    setTimeout(() => {
      map2D.invalidateSize();
    }, 100);
  } else {
    map2DContainer.style.display = 'none';
    map3DContainer.style.display = 'block';
    
    // Refresh Cesium viewer
    setTimeout(() => {
      if (map3D) {
        map3D.resize();
      }
    }, 100);
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
async function loadHeatmapData(url = '/api/heatmap') {
  try {
    const response = await fetch(url);
    const data = await response.json();
    
    if (!data.success) {
      showError('Failed to load heatmap data');
      return;
    }
    
    locationClusters = data.locations;
    
    if (!locationClusters || locationClusters.length === 0) {
      showError('No alumni with coordinates found.');
      return;
    }
    
    // Add markers to both maps
    locationClusters.forEach(location => {
      add2DMarker(location);
      add3DMarker(location);
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
  const entity = map3D.entities.add({
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

// Create popup content for 2D map
function create2DPopupContent(location) {
  const alumniItems = location.sample_alumni
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
        <strong style="color: #333;">Total Alumni:</strong> <span style="color: #667eea; font-weight: 600;">${location.count}</span>
      </div>
      <div style="max-height: 400px; overflow-y: auto;">
        ${alumniItems}
      </div>
    </div>
  `;
}

// Create popup content for 3D map
function create3DPopupContent(location) {
  const alumniItems = location.sample_alumni
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
        <strong style="color: #333;">Total Alumni:</strong> <span style="color: #667eea; font-weight: 600;">${location.count}</span>
      </div>
      <div style="max-height: 400px; overflow-y: auto;">
        ${alumniItems}
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

// Filter by continent
function filterByContinent(continent) {
  const bounds = {
    'North America': [[15, -170], [72, -50]],
    'South America': [[-56, -82], [13, -34]],
    'Europe': [[36, -10], [71, 40]],
    'Asia': [[0, 40], [55, 150]],
    'Africa': [[-35, -20], [37, 52]],
    'Oceania': [[-47, 110], [-10, 180]],
    'Antarctica': [[-90, -180], [-60, 180]]
  };
  
  if (currentMode === '2d' && bounds[continent]) {
    map2D.fitBounds(bounds[continent]);
  } else if (currentMode === '3d' && bounds[continent]) {
    const [[minLat, minLng], [maxLat, maxLng]] = bounds[continent];
    const centerLat = (minLat + maxLat) / 2;
    const centerLng = (minLng + maxLng) / 2;
    
    map3D.camera.flyTo({
      destination: Cesium.Cartesian3.fromDegrees(centerLng, centerLat, 5000000),
      duration: 2
    });
  }
}

// Reset continent filter
function resetContinent() {
  if (currentMode === '2d') {
    map2D.setView([20, 0], 2);
  } else {
    map3D.camera.flyTo({
      destination: Cesium.Cartesian3.fromDegrees(-95.0, 40.0, 15000000),
      orientation: {
        heading: 0.0,
        pitch: Cesium.Math.toRadians(-60),
        roll: 0.0
      },
      duration: 2
    });
  }
}

// Update statistics
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

// Update pie chart
function updatePieChart(locations) {
  const continentCounts = aggregateAlumniByContinent(locations);
  const labels = Object.keys(continentCounts).filter(continent => continentCounts[continent] > 0);
  const data = labels.map(continent => continentCounts[continent]);
  const colors = ['#667eea', '#764ba2', '#f093fb', '#4facfe', '#ffa400', '#43e97b', '#fa709a'];
  
  const chartCanvas = document.getElementById('continentPieChart');
  if (!chartCanvas) return;
  
  if (pieChart) pieChart.destroy();
  
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
          position: 'bottom'
        }
      }
    }
  });
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
