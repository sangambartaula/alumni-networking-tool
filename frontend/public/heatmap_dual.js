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
  
  // Add solar system visualization - realistic orbital positions
  // The globe itself IS Earth at center, other planets orbit around it (simplified model)
  
  // Sun - the star at the center providing light
  map3D.entities.add({
    name: 'Sun',
    position: Cesium.Cartesian3.fromDegrees(-90, 0, 600000000), // Far left with depth
    ellipsoid: {
      radii: new Cesium.Cartesian3(20000000, 20000000, 20000000), // Much smaller for visibility
      material: new Cesium.ColorMaterialProperty(Cesium.Color.YELLOW.brighten(0.5, new Cesium.Color()))
    },
    label: {
      text: 'Sun',
      font: 'bold 20pt sans-serif',
      fillColor: Cesium.Color.YELLOW,
      outlineColor: Cesium.Color.BLACK,
      outlineWidth: 3,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      pixelOffset: new Cesium.Cartesian2(0, -40)
    }
  });
  
  // Moon - Earth's natural satellite with realistic gray color
  map3D.entities.add({
    name: 'Moon',
    position: Cesium.Cartesian3.fromDegrees(-25, 0, 430000000), // Right next to Earth with depth
    ellipsoid: {
      radii: new Cesium.Cartesian3(1737400, 1737400, 1737400), // Moon's actual size
      material: new Cesium.ColorMaterialProperty(Cesium.Color.fromCssColorString('#9E9E9E')) // Darker gray, not white
    },
    label: {
      text: 'Moon',
      font: 'bold 14pt sans-serif',
      fillColor: Cesium.Color.WHITE,
      outlineColor: Cesium.Color.BLACK,
      outlineWidth: 3,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      pixelOffset: new Cesium.Cartesian2(0, -20)
    }
  });
  
  // Mercury - innermost planet, rocky with cratered surface
  map3D.entities.add({
    name: 'Mercury',
    position: Cesium.Cartesian3.fromDegrees(-70, 0, 550000000), // After Sun with depth
    ellipsoid: {
      radii: new Cesium.Cartesian3(2439700, 2439700, 2439700),
      material: new Cesium.ColorMaterialProperty(Cesium.Color.fromCssColorString('#8C7853')) // Grayish-tan
    },
    label: {
      text: 'Mercury',
      font: 'bold 13pt sans-serif',
      fillColor: Cesium.Color.WHITE,
      outlineColor: Cesium.Color.BLACK,
      outlineWidth: 3,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      pixelOffset: new Cesium.Cartesian2(0, -18)
    }
  });
  
  // Venus - second planet, thick atmosphere
  map3D.entities.add({
    name: 'Venus',
    position: Cesium.Cartesian3.fromDegrees(-50, 0, 500000000), // After Mercury with depth
    ellipsoid: {
      radii: new Cesium.Cartesian3(6051800, 6051800, 6051800),
      material: new Cesium.ColorMaterialProperty(Cesium.Color.fromCssColorString('#FFC649')) // Yellowish clouds
    },
    label: {
      text: 'Venus',
      font: 'bold 14pt sans-serif',
      fillColor: Cesium.Color.WHITE,
      outlineColor: Cesium.Color.BLACK,
      outlineWidth: 3,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      pixelOffset: new Cesium.Cartesian2(0, -22)
    }
  });
  
  // Mars - the Red Planet with iron oxide surface
  map3D.entities.add({
    name: 'Mars',
    position: Cesium.Cartesian3.fromDegrees(-10, 0, 400000000), // After Earth with depth
    ellipsoid: {
      radii: new Cesium.Cartesian3(3389500, 3389500, 3389500),
      material: new Cesium.ColorMaterialProperty(Cesium.Color.fromCssColorString('#CD5C5C')) // Rusty red
    },
    label: {
      text: 'Mars',
      font: 'bold 14pt sans-serif',
      fillColor: Cesium.Color.WHITE,
      outlineColor: Cesium.Color.BLACK,
      outlineWidth: 3,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      pixelOffset: new Cesium.Cartesian2(0, -20)
    }
  });
  
  // Jupiter - gas giant with prominent bands
  map3D.entities.add({
    name: 'Jupiter',
    position: Cesium.Cartesian3.fromDegrees(20, 0, 300000000), // After Mars with more distance
    ellipsoid: {
      radii: new Cesium.Cartesian3(69911000, 69911000, 66854000), // Slightly oblate
      material: new Cesium.ColorMaterialProperty(Cesium.Color.fromCssColorString('#C88B3A')) // Orange-tan bands
    },
    label: {
      text: 'Jupiter',
      font: 'bold 16pt sans-serif',
      fillColor: Cesium.Color.WHITE,
      outlineColor: Cesium.Color.BLACK,
      outlineWidth: 3,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      pixelOffset: new Cesium.Cartesian2(0, -30)
    }
  });
  
  // Saturn - ringed gas giant
  const saturnPosition = Cesium.Cartesian3.fromDegrees(50, 0, 200000000); // After Jupiter with more distance
  
  map3D.entities.add({
    name: 'Saturn',
    position: saturnPosition,
    ellipsoid: {
      radii: new Cesium.Cartesian3(58232000, 58232000, 54364000), // Oblate
      material: new Cesium.ColorMaterialProperty(Cesium.Color.fromCssColorString('#FAD5A5')) // Pale gold
    },
    label: {
      text: 'Saturn',
      font: 'bold 15pt sans-serif',
      fillColor: Cesium.Color.WHITE,
      outlineColor: Cesium.Color.BLACK,
      outlineWidth: 3,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      pixelOffset: new Cesium.Cartesian2(0, -35)
    }
  });
  
  // Uranus - ice giant with blue-green methane atmosphere
  map3D.entities.add({
    name: 'Uranus',
    position: Cesium.Cartesian3.fromDegrees(75, 0, 100000000), // After Saturn with more distance
    ellipsoid: {
      radii: new Cesium.Cartesian3(25362000, 25362000, 24973000),
      material: new Cesium.ColorMaterialProperty(Cesium.Color.fromCssColorString('#4FD0E7')) // Cyan-blue
    },
    label: {
      text: 'Uranus',
      font: 'bold 14pt sans-serif',
      fillColor: Cesium.Color.WHITE,
      outlineColor: Cesium.Color.BLACK,
      outlineWidth: 3,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      pixelOffset: new Cesium.Cartesian2(0, -22)
    }
  });
  
  // Neptune - outermost ice giant with deep blue color
  map3D.entities.add({
    name: 'Neptune',
    position: Cesium.Cartesian3.fromDegrees(100, 0, 50000000), // After Uranus with more distance
    ellipsoid: {
      radii: new Cesium.Cartesian3(24622000, 24622000, 24341000),
      material: new Cesium.ColorMaterialProperty(Cesium.Color.fromCssColorString('#4169E1')) // Deep blue
    },
    label: {
      text: 'Neptune',
      font: 'bold 14pt sans-serif',
      fillColor: Cesium.Color.WHITE,
      outlineColor: Cesium.Color.BLACK,
      outlineWidth: 3,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      pixelOffset: new Cesium.Cartesian2(0, -22)
    }
  });
  
  // Set initial camera view to show full Earth globe
  map3D.camera.setView({
    destination: Cesium.Cartesian3.fromDegrees(0, 20, 465000000), // Just above Earth's position at 450M
    orientation: {
      heading: 0.0,
      pitch: Cesium.Math.toRadians(-90),
      roll: 0.0
    }
  });
  
  // Store default Earth view for easy return
  window.earthView = {
    destination: Cesium.Cartesian3.fromDegrees(0, 20, 465000000),
    orientation: {
      heading: 0.0,
      pitch: Cesium.Math.toRadians(-90),
      roll: 0.0
    }
  };
  
  // Enable camera controls so users can navigate
  map3D.scene.screenSpaceCameraController.enableRotate = true;
  map3D.scene.screenSpaceCameraController.enableZoom = true;
  map3D.scene.screenSpaceCameraController.enableTilt = true;
  map3D.scene.screenSpaceCameraController.enableLook = true;
  
  // Set minimum and maximum zoom distances
  map3D.scene.screenSpaceCameraController.minimumZoomDistance = 1000; // Can't zoom closer than 1km
  map3D.scene.screenSpaceCameraController.maximumZoomDistance = 10000000000000; // 10 trillion meters to see full solar system
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

// Show Solar System view
function showSolarSystem() {
  // Function to zoom to solar system
  const zoomToSolarSystem = () => {
    if (map3D) {
      // View solar system from directly above to see all planets
      map3D.camera.flyTo({
        destination: Cesium.Cartesian3.fromDegrees(0, 20, 600000000), // 600 million meters - see all planets together
        orientation: {
          heading: Cesium.Math.toRadians(0), 
          pitch: Cesium.Math.toRadians(-90), // Look straight down like Earth view
          roll: 0.0
        },
        duration: 4
      });
    }
  };
  
  if (currentMode === '2d') {
    // Switch to 3D for solar system view
    const toggle3DBtn = document.querySelector('[data-mode="3d"]');
    if (toggle3DBtn) {
      toggle3DBtn.click();
      // Wait for 3D mode to initialize before zooming
      setTimeout(zoomToSolarSystem, 500);
    }
  } else {
    // Already in 3D mode, zoom immediately
    zoomToSolarSystem();
  }
}

// Reset to Earth view
function resetContinent() {
  if (currentMode === '2d') {
    // Reset 2D map
    map2D.setView([20, 0], 2);
  } else {
    // Return to Earth view in 3D
    if (map3D && window.earthView) {
      map3D.camera.flyTo({
        destination: window.earthView.destination,
        orientation: window.earthView.orientation,
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
