// heatmap_dual.js - Dual map system with both 2D Leaflet and 3D Cesium

// Set Cesium Ion access token
Cesium.Ion.defaultAccessToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJlYWE1OWUxNy1mMWZiLTQzYjYtYTQ0OS1kMWFjYmFkNjc5YzciLCJpZCI6NTc3MzMsImlhdCI6MTYyNzg0NTE4Mn0.XcKpgANiY19MC4bdFUXMVEBToBmqS8kuYpUlxJHYZxk';

let map2D;
let map3D;
let locationClusters = [];
let currentMode = '2d'; // Start with 2D
let markers2D = [];
let entities3D = [];

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
  initialize2DMap();
  initialize3DMap();
  loadHeatmapData();
  add2D3DToggle();
  addLayerControls();
  addCustomFullscreenButton();
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
  map3D.selectedEntityChanged.addEventListener(function() {
    const entity = map3D.selectedEntity;
    if (entity && entity.position) {
      setTimeout(() => {
        const zoomFunction = function(e) {
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
  const cesiumToolbar = document.querySelector('.cesium-viewer-toolbar');
  
  if (mode === '2d') {
    map2DContainer.style.display = 'block';
    map3DContainer.style.display = 'none';
    
    // Show Cesium fullscreen button for 2D mode too
    if (cesiumToolbar) {
      cesiumToolbar.style.display = 'block';
    }
    
    // Refresh Leaflet map
    setTimeout(() => {
      map2D.invalidateSize();
    }, 100);
  } else {
    map2DContainer.style.display = 'none';
    map3DContainer.style.display = 'block';
    
    // Show Cesium fullscreen button for 3D mode
    if (cesiumToolbar) {
      cesiumToolbar.style.display = 'block';
    }
    
    // Refresh Cesium viewer
    setTimeout(() => {
      if (map3D) {
        map3D.resize();
      }
    }, 100);
  }
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

// Reset to Earth view
function resetContinent() {
  if (currentMode === '2d') {
    // Reset 2D map
    map2D.setView([20, 0], 2);
  } else {
    // Return to default Earth view in 3D
    if (map3D) {
      map3D.camera.flyTo({
        destination: Cesium.Cartesian3.fromDegrees(0, 20, 20000000),
        orientation: {
          heading: 0.0,
          pitch: Cesium.Math.toRadians(-90),
          roll: 0.0
        },
        duration: 2
      });
    }
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
  } else if (currentMode === '3d' && bounds[continentName]) {
    const [[minLat, minLng], [maxLat, maxLng]] = bounds[continentName];
    const centerLat = (minLat + maxLat) / 2;
    const centerLng = (minLng + maxLng) / 2;
    
    map3D.camera.flyTo({
      destination: Cesium.Cartesian3.fromDegrees(centerLng, centerLat, 5000000),
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
  } else {
    map3D.camera.flyTo({
      destination: Cesium.Cartesian3.fromDegrees(loc.longitude, loc.latitude, 2000000),
      duration: 2
    });
  }
}

// Zoom to selected autocomplete result from Nominatim
function zoomToSearchLocation(item) {
  const lat = parseFloat(item.lat);
  const lon = parseFloat(item.lon);

  if (currentMode === '2d') {
    map2D.setView([lat, lon], 8);
  } else {
    map3D.camera.flyTo({
      destination: Cesium.Cartesian3.fromDegrees(lon, lat, 2000000),
      duration: 2
    });
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
