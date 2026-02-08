// analytics.js
// Analytics dashboard functionality

let alumniData = [];
let charts = {};
let currentFilter = null;

// Filter state
let hiddenLocations = new Set();
let hiddenCompanies = new Set();
let allLocations = new Set();
let allCompanies = new Set();

// Initialize analytics on page load
document.addEventListener('DOMContentLoaded', () => {
  loadHiddenFiltersFromStorage();
  initializeAnalyticsFilterUI();
  loadAnalyticsData();

  // Setup modal close handlers
  const modal = document.getElementById('alumniModal');
  const closeBtn = document.getElementById('closeModalBtn');

  closeBtn?.addEventListener('click', closeModal);

  // Close modal when clicking outside
  modal?.addEventListener('click', (e) => {
    if (e.target === modal) {
      closeModal();
    }
  });

  // Close modal with Escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      closeModal();
    }
  });
});

// Filter Management Functions
function saveHiddenFiltersToStorage() {
  localStorage.setItem('analyticsHiddenLocations', JSON.stringify(Array.from(hiddenLocations)));
  localStorage.setItem('analyticsHiddenCompanies', JSON.stringify(Array.from(hiddenCompanies)));
}

function loadHiddenFiltersFromStorage() {
  try {
    const savedLocations = localStorage.getItem('analyticsHiddenLocations');
    const savedCompanies = localStorage.getItem('analyticsHiddenCompanies');

    if (savedLocations) {
      hiddenLocations = new Set(JSON.parse(savedLocations));
    }
    if (savedCompanies) {
      hiddenCompanies = new Set(JSON.parse(savedCompanies));
    }
  } catch (error) {
    console.error('Error loading filters from storage:', error);
    hiddenLocations = new Set();
    hiddenCompanies = new Set();
  }
}

function initializeAnalyticsFilterUI() {
  const toggleBtn = document.getElementById('analyticsFilterToggleBtn');
  const closeBtn = document.getElementById('analyticsFilterCloseBtn');
  const backdrop = document.getElementById('analyticsFilterBackdrop');
  const panel = document.getElementById('analyticsFilterPanel');
  const clearBtn = document.getElementById('analyticsClearAllFiltersBtn');

  const locationInput = document.getElementById('analyticsFilterLocationInput');
  const companyInput = document.getElementById('analyticsFilterCompanyInput');
  const locationSuggestions = document.getElementById('analyticsLocationSuggestionsDropdown');
  const companySuggestions = document.getElementById('analyticsCompanySuggestionsDropdown');
  const addLocationBtn = document.getElementById('analyticsAddLocationFilterBtn');
  const addCompanyBtn = document.getElementById('analyticsAddCompanyFilterBtn');

  // Toggle filter panel
  toggleBtn?.addEventListener('click', () => {
    panel?.classList.add('active');
    backdrop?.classList.add('active');
    showRecommendations(); // Show recommendations when panel opens
  });

  // Close filter panel
  const closePanel = () => {
    panel?.classList.remove('active');
    backdrop?.classList.remove('active');
    // Clear input fields and hide suggestions
    if (locationInput) locationInput.value = '';
    if (companyInput) companyInput.value = '';
    if (locationSuggestions) locationSuggestions.style.display = 'none';
    if (companySuggestions) companySuggestions.style.display = 'none';
  };

  closeBtn?.addEventListener('click', closePanel);
  backdrop?.addEventListener('click', closePanel);

  // Clear all filters
  clearBtn?.addEventListener('click', () => {
    hiddenLocations.clear();
    hiddenCompanies.clear();
    saveHiddenFiltersToStorage();
    updateAnalyticsFilterUI();
    renderAnalytics();
  });

  // Add Location Button Click
  addLocationBtn?.addEventListener('click', () => {
    const value = locationInput.value.trim();
    if (value) {
      // Find exact or fuzzy match
      const matchingLocation = findBestMatch(value, allLocations);
      if (matchingLocation && !hiddenLocations.has(matchingLocation)) {
        addLocationFilter(matchingLocation);
        locationInput.value = '';
        locationSuggestions.style.display = 'none';
        showRecommendations();
      } else {
        // Show error or suggestion
        alert(matchingLocation ? 'Location already filtered!' : 'Location not found. Please select from suggestions.');
      }
    }
  });

  // Add Company Button Click
  addCompanyBtn?.addEventListener('click', () => {
    const value = companyInput.value.trim();
    if (value) {
      // Find exact or fuzzy match
      const matchingCompany = findBestMatch(value, allCompanies);
      if (matchingCompany && !hiddenCompanies.has(matchingCompany)) {
        addCompanyFilter(matchingCompany);
        companyInput.value = '';
        companySuggestions.style.display = 'none';
        showRecommendations();
      } else {
        // Show error or suggestion
        alert(matchingCompany ? 'Company already filtered!' : 'Company not found. Please select from suggestions.');
      }
    }
  });

  // Location input autocomplete
  locationInput?.addEventListener('input', (e) => {
    const value = e.target.value.trim();
    if (value.length > 0) {
      const suggestions = Array.from(allLocations)
        .filter(loc => loc.toLowerCase().includes(value.toLowerCase()) && !hiddenLocations.has(loc))
        .sort((a, b) => {
          // Prioritize exact matches and starts-with
          const aLower = a.toLowerCase();
          const bLower = b.toLowerCase();
          const valueLower = value.toLowerCase();

          // Exact match first
          if (aLower === valueLower) return -1;
          if (bLower === valueLower) return 1;

          // Starts with match second
          if (aLower.startsWith(valueLower) && !bLower.startsWith(valueLower)) return -1;
          if (!aLower.startsWith(valueLower) && bLower.startsWith(valueLower)) return 1;

          return a.localeCompare(b);
        })
        .slice(0, 15);

      if (suggestions.length > 0) {
        locationSuggestions.innerHTML = suggestions
          .map(loc => {
            const escapedLoc = loc.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
            return `<div class="analytics-suggestion-item" data-value="${escapedLoc}"><span class="analytics-suggestion-text">${highlightMatch(loc, value)}</span></div>`;
          })
          .join('');
        locationSuggestions.style.display = 'block';
      } else {
        locationSuggestions.innerHTML = '<div class="analytics-suggestion-no-results">No matching locations found</div>';
        locationSuggestions.style.display = 'block';
      }
    } else {
      showRecommendations(); // Show recommendations when input is cleared
    }
  });

  // Company input autocomplete
  companyInput?.addEventListener('input', (e) => {
    const value = e.target.value.trim();
    if (value.length > 0) {
      const suggestions = Array.from(allCompanies)
        .filter(comp => comp.toLowerCase().includes(value.toLowerCase()) && !hiddenCompanies.has(comp))
        .sort((a, b) => {
          const aLower = a.toLowerCase();
          const bLower = b.toLowerCase();
          const valueLower = value.toLowerCase();

          // Exact match first
          if (aLower === valueLower) return -1;
          if (bLower === valueLower) return 1;

          // Starts with match second
          if (aLower.startsWith(valueLower) && !bLower.startsWith(valueLower)) return -1;
          if (!aLower.startsWith(valueLower) && bLower.startsWith(valueLower)) return 1;

          return a.localeCompare(b);
        })
        .slice(0, 15);

      if (suggestions.length > 0) {
        companySuggestions.innerHTML = suggestions
          .map(comp => {
            const escapedComp = comp.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
            return `<div class="analytics-suggestion-item" data-value="${escapedComp}"><span class="analytics-suggestion-text">${highlightMatch(comp, value)}</span></div>`;
          })
          .join('');
        companySuggestions.style.display = 'block';
      } else {
        companySuggestions.innerHTML = '<div class="analytics-suggestion-no-results">No matching companies found</div>';
        companySuggestions.style.display = 'block';
      }
    } else {
      showRecommendations(); // Show recommendations when input is cleared
    }
  });

  // Location suggestion click
  locationSuggestions?.addEventListener('click', (e) => {
    if (e.target.classList.contains('analytics-suggestion-item')) {
      const location = e.target.getAttribute('data-value');
      addLocationFilter(location);
      locationInput.value = '';
      showRecommendations(); // Show recommendations after adding filter
    }
  });

  // Company suggestion click
  companySuggestions?.addEventListener('click', (e) => {
    if (e.target.classList.contains('analytics-suggestion-item')) {
      const company = e.target.getAttribute('data-value');
      addCompanyFilter(company);
      companyInput.value = '';
      showRecommendations(); // Show recommendations after adding filter
    }
  });

  // Enter key to add filter
  locationInput?.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
      const value = e.target.value.trim();
      if (value) {
        // Find matching location (case-insensitive)
        const matchingLocation = Array.from(allLocations).find(loc =>
          loc.toLowerCase() === value.toLowerCase()
        );
        if (matchingLocation) {
          addLocationFilter(matchingLocation);
          locationInput.value = '';
          showRecommendations();
        }
      }
    }
  });

  companyInput?.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
      const value = e.target.value.trim();
      if (value) {
        // Find matching company (case-insensitive)
        const matchingCompany = Array.from(allCompanies).find(comp =>
          comp.toLowerCase() === value.toLowerCase()
        );
        if (matchingCompany) {
          addCompanyFilter(matchingCompany);
          companyInput.value = '';
          showRecommendations();
        }
      }
    }
  });

  // Focus handlers to show recommendations
  locationInput?.addEventListener('focus', () => {
    if (!locationInput.value.trim()) {
      showRecommendations();
    }
  });

  companyInput?.addEventListener('focus', () => {
    if (!companyInput.value.trim()) {
      showRecommendations();
    }
  });

  updateAnalyticsFilterUI();
}

// Helper function to find best matching item (case-insensitive)
function findBestMatch(input, itemSet) {
  const inputLower = input.toLowerCase();
  const items = Array.from(itemSet);

  // Try exact match first
  const exactMatch = items.find(item => item.toLowerCase() === inputLower);
  if (exactMatch) return exactMatch;

  // Try starts-with match
  const startsWithMatch = items.find(item => item.toLowerCase().startsWith(inputLower));
  if (startsWithMatch) return startsWithMatch;

  // Try contains match (return first match)
  const containsMatch = items.find(item => item.toLowerCase().includes(inputLower));
  return containsMatch || null;
}

// Helper function to highlight matching text in suggestions
function highlightMatch(text, search) {
  if (!search) return text;

  const regex = new RegExp(`(${search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
  return text.replace(regex, '<strong>$1</strong>');
}

function showRecommendations() {
  const locationInput = document.getElementById('analyticsFilterLocationInput');
  const companyInput = document.getElementById('analyticsFilterCompanyInput');
  const locationSuggestions = document.getElementById('analyticsLocationSuggestionsDropdown');
  const companySuggestions = document.getElementById('analyticsCompanySuggestionsDropdown');

  // Show top locations if location input is focused and empty
  if (locationInput && document.activeElement === locationInput && !locationInput.value.trim()) {
    // Get top 10 locations by alumni count
    const locationCounts = {};
    alumniData.forEach(alumni => {
      if (alumni.location && !hiddenLocations.has(alumni.location)) {
        locationCounts[alumni.location] = (locationCounts[alumni.location] || 0) + 1;
      }
    });

    const topLocations = Object.entries(locationCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([loc, count]) => ({ name: loc, count }));

    if (topLocations.length > 0 && locationSuggestions) {
      locationSuggestions.innerHTML = '<div class="analytics-suggestion-header">Popular Locations (click to hide)</div>' +
        topLocations
          .map(({ name, count }) => {
            const escapedName = name.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
            return `<div class="analytics-suggestion-item" data-value="${escapedName}"><span class="analytics-suggestion-text">${name}</span> <span class="analytics-suggestion-count">(${count})</span></div>`;
          })
          .join('');
      locationSuggestions.style.display = 'block';
    }
  }

  // Show top companies if company input is focused and empty
  if (companyInput && document.activeElement === companyInput && !companyInput.value.trim()) {
    // Get top 10 companies by alumni count
    const companyCounts = {};
    alumniData.forEach(alumni => {
      if (alumni.company && !hiddenCompanies.has(alumni.company)) {
        companyCounts[alumni.company] = (companyCounts[alumni.company] || 0) + 1;
      }
    });

    const topCompanies = Object.entries(companyCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([comp, count]) => ({ name: comp, count }));

    if (topCompanies.length > 0 && companySuggestions) {
      companySuggestions.innerHTML = '<div class="analytics-suggestion-header">Popular Companies (click to hide)</div>' +
        topCompanies
          .map(({ name, count }) => {
            const escapedName = name.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
            return `<div class="analytics-suggestion-item" data-value="${escapedName}"><span class="analytics-suggestion-text">${name}</span> <span class="analytics-suggestion-count">(${count})</span></div>`;
          })
          .join('');
      companySuggestions.style.display = 'block';
    }
  }
}

function addLocationFilter(location) {
  hiddenLocations.add(location);
  saveHiddenFiltersToStorage();
  updateAnalyticsFilterUI();
  renderAnalytics();
}

function addCompanyFilter(company) {
  hiddenCompanies.add(company);
  saveHiddenFiltersToStorage();
  updateAnalyticsFilterUI();
  renderAnalytics();
}

function removeLocationFilter(location) {
  hiddenLocations.delete(location);
  saveHiddenFiltersToStorage();
  updateAnalyticsFilterUI();
  renderAnalytics();
}

function removeCompanyFilter(company) {
  hiddenCompanies.delete(company);
  saveHiddenFiltersToStorage();
  updateAnalyticsFilterUI();
  renderAnalytics();
}

function updateAnalyticsFilterUI() {
  const locationTagsContainer = document.getElementById('analyticsLocationFilterTags');
  const companyTagsContainer = document.getElementById('analyticsCompanyFilterTags');
  const locationCountSpan = document.getElementById('analyticsLocationFilterCount');
  const companyCountSpan = document.getElementById('analyticsCompanyFilterCount');
  const badge = document.getElementById('analyticsFilterBadge');
  const clearBtn = document.getElementById('analyticsClearAllFiltersBtn');

  // Update badge count
  const totalFilters = hiddenLocations.size + hiddenCompanies.size;
  if (badge) {
    badge.textContent = totalFilters;
    badge.style.display = totalFilters > 0 ? 'inline-block' : 'none';
  }

  // Update count spans
  if (locationCountSpan) {
    locationCountSpan.textContent = hiddenLocations.size;
  }
  if (companyCountSpan) {
    companyCountSpan.textContent = hiddenCompanies.size;
  }

  // Update clear button state
  if (clearBtn) {
    clearBtn.disabled = totalFilters === 0;
  }

  // Render location tags
  if (locationTagsContainer) {
    if (hiddenLocations.size === 0) {
      locationTagsContainer.innerHTML = '<span class="empty-analytics-filters-message">No locations hidden</span>';
    } else {
      locationTagsContainer.innerHTML = Array.from(hiddenLocations)
        .map(loc => `
          <span class="analytics-filter-tag">
            <span>${loc}</span>
            <button class="analytics-filter-tag-remove" onclick="removeLocationFilter('${loc.replace(/'/g, "\\'")}')">×</button>
          </span>
        `).join('');
    }
  }

  // Render company tags
  if (companyTagsContainer) {
    if (hiddenCompanies.size === 0) {
      companyTagsContainer.innerHTML = '<span class="empty-analytics-filters-message">No companies hidden</span>';
    } else {
      companyTagsContainer.innerHTML = Array.from(hiddenCompanies)
        .map(comp => `
          <span class="analytics-filter-tag">
            <span>${comp}</span>
            <button class="analytics-filter-tag-remove" onclick="removeCompanyFilter('${comp.replace(/'/g, "\\'")}')">×</button>
          </span>
        `).join('');
    }
  }
}

function buildAnalyticsAutocomplete(data) {
  allLocations.clear();
  allCompanies.clear();

  data.forEach(alumni => {
    // Use the location field directly as it's already formatted
    if (alumni.location && alumni.location.trim() !== '') {
      allLocations.add(alumni.location);
    }
    if (alumni.company && alumni.company.trim() !== '') {
      allCompanies.add(alumni.company);
    }
  });
}

function filterAlumniData(data) {
  return data.filter(alumni => {
    // Filter by location - check against the actual location field
    if (alumni.location && hiddenLocations.has(alumni.location)) {
      return false;
    }

    // Filter by company
    if (alumni.company && hiddenCompanies.has(alumni.company)) {
      return false;
    }

    return true;
  });
}

// Load all alumni data for analytics
async function loadAnalyticsData() {
  try {
    const response = await fetch('/api/alumni');
    const data = await response.json();

    if (data.success) {
      alumniData = data.alumni;
      buildAnalyticsAutocomplete(alumniData);
      updateAnalyticsFilterUI();
      renderAnalytics();
    } else {
      console.error('Failed to load alumni data:', data.error);
    }
  } catch (error) {
    console.error('Error loading analytics data:', error);
  }
}

// Render all analytics components
function renderAnalytics() {
  const filteredData = filterAlumniData(alumniData);
  updateStatistics(filteredData);
  renderTopJobs(filteredData);
  renderJobPieChart(filteredData);
  renderCompanyPieChart(filteredData);
  renderLocationPieChart(filteredData);
  renderIndustryPieChart(filteredData);
  renderGraduationLineChart(filteredData);
  renderTopCompaniesTable(filteredData);
  renderTopLocationsTable(filteredData);
}

// Update summary statistics
function updateStatistics() {
  const totalAlumni = alumniData.length;
  const uniqueCompanies = new Set(alumniData.map(a => a.company).filter(c => c)).size;
  const uniqueLocations = new Set(alumniData.map(a => a.location).filter(l => l && l !== 'Not Found')).size;
  const uniqueJobs = new Set(alumniData.map(a => a.current_job_title).filter(j => j)).size;

  document.getElementById('totalAlumni').textContent = totalAlumni;
  document.getElementById('totalCompanies').textContent = uniqueCompanies;
  document.getElementById('totalLocations').textContent = uniqueLocations;
  document.getElementById('totalJobs').textContent = uniqueJobs;
}

// Get top N items from a frequency map
function getTopItems(items, topN = 5) {
  const frequency = {};
  items.forEach(item => {
    if (item && item.trim()) {
      frequency[item] = (frequency[item] || 0) + 1;
    }
  });

  return Object.entries(frequency)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN);
}

// Render top 5 jobs list
function renderTopJobs(data = alumniData) {
  const jobs = data.map(a => a.current_job_title).filter(j => j);
  const topJobs = getTopItems(jobs, 5);
  const maxCount = topJobs[0]?.[1] || 1;

  const container = document.getElementById('topJobsList');
  container.innerHTML = topJobs.map(([job, count], index) => {
    const percentage = (count / maxCount) * 100;
    return `
      <div class="job-item" data-job="${job}" style="cursor: pointer;" title="Click to see alumni">
        <div class="job-rank">${index + 1}</div>
        <div class="job-details">
          <div class="job-title">${job}</div>
          <div class="job-count">${count} alumni</div>
        </div>
        <div class="job-bar-container">
          <div class="job-bar" style="width: ${percentage}%"></div>
        </div>
      </div>
    `;
  }).join('');

  // Add click handlers for each job item
  container.querySelectorAll('.job-item').forEach(item => {
    item.addEventListener('click', () => {
      const jobTitle = item.dataset.job;
      filterAlumni('job', jobTitle);
    });
  });
}

// Generate color palette
function generateColors(count) {
  const colors = [
    '#667eea', '#764ba2', '#f093fb', '#4facfe', '#ffa400',
    '#43e97b', '#fa709a', '#feca57', '#48dbfb', '#1dd1a1',
    '#5f27cd', '#ee5a6f', '#0984e3', '#00d2d3', '#ff6b6b'
  ];

  const result = [];
  for (let i = 0; i < count; i++) {
    result.push(colors[i % colors.length]);
  }
  return result;
}

// Render job title pie chart
function renderJobPieChart(data = alumniData) {
  const jobs = data.map(a => a.current_job_title).filter(j => j);
  const topJobs = getTopItems(jobs, 10);

  const labels = topJobs.map(([job]) => job);
  const jobChartData = topJobs.map(([, count]) => count);
  const colors = generateColors(labels.length);

  const ctx = document.getElementById('jobPieChart').getContext('2d');

  if (charts.jobPie) charts.jobPie.destroy();

  charts.jobPie = new Chart(ctx, {
    type: 'pie',
    data: {
      labels: labels,
      datasets: [{
        data: jobChartData,
        backgroundColor: colors,
        borderColor: '#fff',
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'bottom',
          labels: {
            padding: 10,
            font: { size: 11 }
          }
        },
        tooltip: {
          callbacks: {
            label: function (context) {
              const total = context.dataset.data.reduce((a, b) => a + b, 0);
              const percentage = ((context.parsed / total) * 100).toFixed(1);
              return `${context.label}: ${context.parsed} (${percentage}%)`;
            },
            afterLabel: function () {
              return 'Click to view alumni';
            }
          }
        }
      },
      onClick: (event, elements) => {
        if (elements.length > 0) {
          const index = elements[0].index;
          const jobTitle = labels[index];
          filterAlumni('job', jobTitle);
        }
      }
    }
  });
}

// Render company pie chart
function renderCompanyPieChart(data = alumniData) {
  const companies = data.map(a => a.company).filter(c => c);
  const topCompanies = getTopItems(companies, 10);

  const labels = topCompanies.map(([company]) => company);
  const companyChartData = topCompanies.map(([, count]) => count);
  const colors = generateColors(labels.length);

  const ctx = document.getElementById('companyPieChart').getContext('2d');

  if (charts.companyPie) charts.companyPie.destroy();

  charts.companyPie = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: labels,
      datasets: [{
        data: companyChartData,
        backgroundColor: colors,
        borderColor: '#fff',
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'bottom',
          labels: {
            padding: 10,
            font: { size: 11 }
          }
        },
        tooltip: {
          callbacks: {
            label: function (context) {
              const total = context.dataset.data.reduce((a, b) => a + b, 0);
              const percentage = ((context.parsed / total) * 100).toFixed(1);
              return `${context.label}: ${context.parsed} (${percentage}%)`;
            },
            afterLabel: function () {
              return 'Click to view alumni';
            }
          }
        }
      },
      onClick: (event, elements) => {
        if (elements.length > 0) {
          const index = elements[0].index;
          const company = labels[index];
          filterAlumni('company', company);
        }
      }
    }
  });
}

// Render location pie chart
function renderLocationPieChart(data = alumniData) {
  const locations = data.map(a => a.location).filter(l => l);
  const topLocations = getTopItems(locations, 10);

  const labels = topLocations.map(([location]) => location);
  const locationChartData = topLocations.map(([, count]) => count);
  const colors = generateColors(labels.length);

  const ctx = document.getElementById('locationPieChart').getContext('2d');

  if (charts.locationPie) charts.locationPie.destroy();

  charts.locationPie = new Chart(ctx, {
    type: 'pie',
    data: {
      labels: labels,
      datasets: [{
        data: locationChartData,
        backgroundColor: colors,
        borderColor: '#fff',
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'bottom',
          labels: {
            padding: 10,
            font: { size: 11 }
          }
        },
        tooltip: {
          callbacks: {
            label: function (context) {
              const total = context.dataset.data.reduce((a, b) => a + b, 0);
              const percentage = ((context.parsed / total) * 100).toFixed(1);
              return `${context.label}: ${context.parsed} (${percentage}%)`;
            },
            afterLabel: function () {
              return 'Click to view alumni';
            }
          }
        }
      },
      onClick: (event, elements) => {
        if (elements.length > 0) {
          const index = elements[0].index;
          const location = labels[index];
          filterAlumni('location', location);
        }
      }
    }
  });
}

// Extract industry from headline or job title
function extractIndustry(alumni) {
  const headline = (alumni.headline || '').toLowerCase();
  const jobTitle = (alumni.current_job_title || '').toLowerCase();
  const text = `${headline} ${jobTitle}`;

  // Simple keyword matching for industries
  if (text.includes('software') || text.includes('developer') || text.includes('engineer')) return 'Engineering';
  if (text.includes('data') || text.includes('analyst')) return 'Data & Analytics';
  if (text.includes('product') || text.includes('manager')) return 'Product Management';
  if (text.includes('design') || text.includes('ux') || text.includes('ui')) return 'Design';
  if (text.includes('marketing') || text.includes('sales')) return 'Marketing & Sales';
  if (text.includes('finance') || text.includes('accounting')) return 'Finance';
  if (text.includes('research') || text.includes('scientist')) return 'Research & Science';
  if (text.includes('consult')) return 'Consulting';
  if (text.includes('education') || text.includes('teacher')) return 'Education';

  return 'Other';
}

// Render industry pie chart
function renderIndustryPieChart(data = alumniData) {
  const industries = data.map(a => extractIndustry(a));
  const topIndustries = getTopItems(industries, 10);

  const labels = topIndustries.map(([industry]) => industry);
  const chartData = topIndustries.map(([, count]) => count);
  const colors = generateColors(labels.length);

  const ctx = document.getElementById('industryPieChart').getContext('2d');

  if (charts.industryPie) charts.industryPie.destroy();

  charts.industryPie = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: labels,
      datasets: [{
        data: chartData,
        backgroundColor: colors,
        borderColor: '#fff',
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'bottom',
          labels: {
            padding: 10,
            font: { size: 11 }
          }
        },
        tooltip: {
          callbacks: {
            label: function (context) {
              const total = context.dataset.data.reduce((a, b) => a + b, 0);
              const percentage = ((context.parsed / total) * 100).toFixed(1);
              return `${context.label}: ${context.parsed} (${percentage}%)`;
            },
            afterLabel: function () {
              return 'Click to view alumni';
            }
          }
        }
      },
      onClick: (event, elements) => {
        if (elements.length > 0) {
          const index = elements[0].index;
          const industry = labels[index];
          filterAlumni('industry', industry);
        }
      }
    }
  });
}

// Render graduation year line chart
function renderGraduationLineChart(data = alumniData) {
  const years = data.map(a => a.grad_year).filter(y => y);
  const yearFrequency = {};

  years.forEach(year => {
    yearFrequency[year] = (yearFrequency[year] || 0) + 1;
  });

  const sortedYears = Object.keys(yearFrequency).sort((a, b) => a - b);
  const counts = sortedYears.map(year => yearFrequency[year]);

  const ctx = document.getElementById('graduationLineChart').getContext('2d');

  if (charts.graduationLine) charts.graduationLine.destroy();

  charts.graduationLine = new Chart(ctx, {
    type: 'line',
    data: {
      labels: sortedYears,
      datasets: [{
        label: 'Number of Graduates',
        data: counts,
        borderColor: '#667eea',
        backgroundColor: 'rgba(102, 126, 234, 0.1)',
        borderWidth: 3,
        fill: true,
        tension: 0.4,
        pointBackgroundColor: '#667eea',
        pointBorderColor: '#fff',
        pointBorderWidth: 2,
        pointRadius: 5,
        pointHoverRadius: 7
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: true,
          position: 'top'
        },
        tooltip: {
          mode: 'index',
          intersect: false,
          callbacks: {
            afterLabel: function () {
              return 'Click to view graduates';
            }
          }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            stepSize: 1
          },
          title: {
            display: true,
            text: 'Number of Alumni'
          }
        },
        x: {
          title: {
            display: true,
            text: 'Graduation Year'
          }
        }
      },
      onClick: (event, elements) => {
        if (elements.length > 0) {
          const index = elements[0].index;
          const year = sortedYears[index];
          filterAlumni('year', year);
        }
      }
    }
  });
}

// Render top companies table
function renderTopCompaniesTable(data = alumniData) {
  const companies = data.map(a => a.company).filter(c => c);
  const topCompanies = getTopItems(companies, 10);

  const tbody = document.getElementById('topCompaniesBody');
  tbody.innerHTML = topCompanies.map(([company, count], index) => {
    let badgeClass = 'default';
    if (index === 0) badgeClass = 'gold';
    else if (index === 1) badgeClass = 'silver';
    else if (index === 2) badgeClass = 'bronze';

    return `
      <tr>
        <td><span class="rank-badge ${badgeClass}">${index + 1}</span></td>
        <td>${company}</td>
        <td><strong>${count}</strong></td>
      </tr>
    `;
  }).join('');
}

// Render top locations table
function renderTopLocationsTable(data = alumniData) {
  const locations = data.map(a => a.location).filter(l => l);
  const topLocations = getTopItems(locations, 10);

  const tbody = document.getElementById('topLocationsBody');
  tbody.innerHTML = topLocations.map(([location, count], index) => {
    let badgeClass = 'default';
    if (index === 0) badgeClass = 'gold';
    else if (index === 1) badgeClass = 'silver';
    else if (index === 2) badgeClass = 'bronze';

    return `
      <tr>
        <td><span class="rank-badge ${badgeClass}">${index + 1}</span></td>
        <td>${location}</td>
        <td><strong>${count}</strong></td>
      </tr>
    `;
  }).join('');
}

// Filter alumni based on chart clicks
function filterAlumni(filterType, filterValue) {
  let filtered = [];
  let filterTitle = '';
  let filterDesc = '';

  switch (filterType) {
    case 'job':
      filtered = alumniData.filter(a => a.current_job_title === filterValue);
      filterTitle = `Alumni with Job Title: ${filterValue}`;
      filterDesc = `Showing ${filtered.length} alumni working as ${filterValue}`;
      break;
    case 'company':
      filtered = alumniData.filter(a => a.company === filterValue);
      filterTitle = `Alumni at ${filterValue}`;
      filterDesc = `Showing ${filtered.length} alumni working at ${filterValue}`;
      break;
    case 'location':
      filtered = alumniData.filter(a => a.location === filterValue);
      filterTitle = `Alumni in ${filterValue}`;
      filterDesc = `Showing ${filtered.length} alumni located in ${filterValue}`;
      break;
    case 'industry':
      filtered = alumniData.filter(a => extractIndustry(a) === filterValue);
      filterTitle = `Alumni in ${filterValue}`;
      filterDesc = `Showing ${filtered.length} alumni working in ${filterValue}`;
      break;
    case 'year':
      filtered = alumniData.filter(a => a.grad_year == filterValue);
      filterTitle = `Class of ${filterValue}`;
      filterDesc = `Showing ${filtered.length} alumni who graduated in ${filterValue}`;
      break;
  }

  currentFilter = { type: filterType, value: filterValue, data: filtered };
  renderFilteredAlumni(filtered, filterTitle, filterDesc);
}

// Render filtered alumni table
// Render filtered alumni table
function renderFilteredAlumni(filtered, title, description) {
  // Update modal content
  document.getElementById('modalTitle').textContent = title;
  document.getElementById('modalDescription').textContent = description;

  // Render table
  const tbody = document.getElementById('filteredAlumniBody');

  if (filtered.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 2rem;">No alumni found</td></tr>';
  } else {
    tbody.innerHTML = filtered.map(alumni => {
      // API returns 'linkedin', but check both just in case
      const linkedinUrl = alumni.linkedin || alumni.linkedin_url || '';
      const profileId = alumni.id || '';

      return `
      <tr data-alumni-id="${profileId}">
        <td><strong>${alumni.name || 'N/A'}</strong></td>
        <td>${alumni.current_job_title || 'N/A'}</td>
        <td>${alumni.company || 'N/A'}</td>
        <td>${alumni.location || 'N/A'}</td>
        <td>${alumni.grad_year || 'N/A'}</td>
        <td class="actions-cell">
          <button class="btn-action view-profile" data-id="${profileId}" title="View full profile">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16">
              <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
              <circle cx="12" cy="12" r="3"></circle>
            </svg>
            Profile
          </button>
          ${linkedinUrl ? `
          <a class="btn-action view-linkedin" href="${linkedinUrl}" target="_blank" rel="noopener noreferrer" title="View LinkedIn">
            <svg viewBox="0 0 24 24" fill="currentColor" width="16" height="16">
              <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/>
            </svg>
            LinkedIn
          </a>
          ` : ''}
        </td>
      </tr>
    `;
    }).join('');
  }

  // Show modal
  openModal();
}

// Add event delegation for the filtered table
// This runs once on init, not every render
document.addEventListener('DOMContentLoaded', () => {
  const tbody = document.getElementById('filteredAlumniBody');
  if (tbody) {
    tbody.addEventListener('click', (e) => {
      // Handle Profile button clicks
      const btn = e.target.closest('.view-profile');
      if (btn) {
        e.preventDefault();
        const alumniId = btn.dataset.id;
        // Use currentFilter.data to find the alumni (more efficient than searching full list)
        const sourceData = (currentFilter && currentFilter.data) ? currentFilter.data : alumniData;
        const alumni = sourceData.find(a => String(a.id) === alumniId);

        if (alumni) {
          showProfileModal(alumni);
        }
      }
    });
  }
});

// Show full profile modal
// Show full profile modal with EDIT capability
function showProfileModal(alumni) {
  // Check if profile modal exists, if not create it
  let profileModal = document.getElementById('profileModal');
  if (!profileModal) {
    profileModal = document.createElement('div');
    profileModal.id = 'profileModal';
    profileModal.className = 'modal profile-modal';
    profileModal.innerHTML = `
      <div class="modal-overlay"></div>
      <div class="modal-content modal-large">
        <div class="modal-header">
          <h2 id="profileModalTitle">Alumni Profile</h2>
          <button id="closeProfileModalBtn" class="close-modal-btn">✕</button>
        </div>
        <div class="modal-body" id="profileModalBody"></div>
        <div class="modal-footer">
          <a id="profileLinkedInBtn" class="btn primary" href="#" target="_blank" rel="noopener noreferrer">
            View on LinkedIn
          </a>
          <button id="closeProfileBtn" class="btn secondary">Close</button>
        </div>
      </div>
    `;
    document.body.appendChild(profileModal);

    // Add event listeners
    profileModal.querySelector('.modal-overlay').addEventListener('click', closeProfileModal);
    profileModal.querySelector('#closeProfileModalBtn').addEventListener('click', closeProfileModal);
    profileModal.querySelector('#closeProfileBtn').addEventListener('click', closeProfileModal);
  }

  const body = profileModal.querySelector('#profileModalBody');
  const linkedinBtn = profileModal.querySelector('#profileLinkedInBtn');

  // Update LinkedIn button
  const linkedinUrl = alumni.linkedin || alumni.linkedin_url;
  if (linkedinUrl) {
    linkedinBtn.href = linkedinUrl;
    linkedinBtn.style.display = 'inline-flex';
  } else {
    linkedinBtn.style.display = 'none';
  }

  // Render content
  body.innerHTML = `
    <div class="profile-grid">
      <div class="profile-section">
        <h3>📋 Basic Information</h3>
        <div class="profile-field"><span class="label">Name:</span> <span class="value">${alumni.name || 'N/A'}</span></div>
        <div class="profile-field"><span class="label">Headline:</span> <span class="value">${alumni.headline || 'N/A'}</span></div>
        <div class="profile-field"><span class="label">Location:</span> <span class="value">${alumni.location || 'N/A'}</span></div>
      </div>
      
      <div class="profile-section">
        <h3>🎓 Education</h3>
        <div class="profile-field"><span class="label">Degree:</span> <span class="value">${alumni.full_degree || alumni.degree || 'N/A'}</span></div>
        <div class="profile-field"><span class="label">Major:</span> <span class="value">${alumni.major || 'N/A'}</span></div>
        <div class="profile-field"><span class="label">Grad Year:</span> <span class="value">${alumni.grad_year || 'N/A'}</span></div>
      </div>

      <div class="profile-section">
        <h3>💼 Current Position</h3>
        <div class="profile-field"><span class="label">Job Title:</span> <span class="value">${alumni.current_job_title || 'N/A'}</span></div>
        <div class="profile-field"><span class="label">Company:</span> <span class="value">${alumni.company || 'N/A'}</span></div>
      </div>
    </div>
  `;

  profileModal.classList.add('show');
}

function closeProfileModal() {
  const modal = document.getElementById('profileModal');
  if (modal) {
    modal.classList.remove('show');
  }
}

// Open modal
function openModal() {
  const modal = document.getElementById('alumniModal');
  modal.classList.add('show');
  document.body.style.overflow = 'hidden'; // Prevent background scrolling
}

// Close modal
function closeModal() {
  const modal = document.getElementById('alumniModal');
  modal.classList.remove('show');
  document.body.style.overflow = ''; // Restore scrolling
  currentFilter = null;
}
