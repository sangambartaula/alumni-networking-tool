// analytics.js
// Analytics dashboard functionality

let alumniData = [];
let charts = {};
let currentFilter = null;

// Initialize analytics on page load
document.addEventListener('DOMContentLoaded', () => {
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

// Load all alumni data for analytics
async function loadAnalyticsData() {
  try {
    const response = await fetch('/api/alumni');
    const data = await response.json();

    if (data.success) {
      alumniData = data.alumni;
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
  updateStatistics();
  renderTopJobs();
  renderJobPieChart();
  renderCompanyPieChart();
  renderLocationPieChart();
  renderIndustryPieChart();
  renderGraduationLineChart();
  renderTopCompaniesTable();
  renderTopLocationsTable();
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
function renderTopJobs() {
  const jobs = alumniData.map(a => a.current_job_title).filter(j => j);
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
function renderJobPieChart() {
  const jobs = alumniData.map(a => a.current_job_title).filter(j => j);
  const topJobs = getTopItems(jobs, 10);

  const labels = topJobs.map(([job]) => job);
  const data = topJobs.map(([, count]) => count);
  const colors = generateColors(labels.length);

  const ctx = document.getElementById('jobPieChart').getContext('2d');

  if (charts.jobPie) charts.jobPie.destroy();

  charts.jobPie = new Chart(ctx, {
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
function renderCompanyPieChart() {
  const companies = alumniData.map(a => a.company).filter(c => c);
  const topCompanies = getTopItems(companies, 10);

  const labels = topCompanies.map(([company]) => company);
  const data = topCompanies.map(([, count]) => count);
  const colors = generateColors(labels.length);

  const ctx = document.getElementById('companyPieChart').getContext('2d');

  if (charts.companyPie) charts.companyPie.destroy();

  charts.companyPie = new Chart(ctx, {
    type: 'doughnut',
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
function renderLocationPieChart() {
  const locations = alumniData.map(a => a.location).filter(l => l);
  const topLocations = getTopItems(locations, 10);

  const labels = topLocations.map(([location]) => location);
  const data = topLocations.map(([, count]) => count);
  const colors = generateColors(labels.length);

  const ctx = document.getElementById('locationPieChart').getContext('2d');

  if (charts.locationPie) charts.locationPie.destroy();

  charts.locationPie = new Chart(ctx, {
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
function renderIndustryPieChart() {
  const industries = alumniData.map(a => extractIndustry(a));
  const topIndustries = getTopItems(industries, 10);

  const labels = topIndustries.map(([industry]) => industry);
  const data = topIndustries.map(([, count]) => count);
  const colors = generateColors(labels.length);

  const ctx = document.getElementById('industryPieChart').getContext('2d');

  if (charts.industryPie) charts.industryPie.destroy();

  charts.industryPie = new Chart(ctx, {
    type: 'doughnut',
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
function renderGraduationLineChart() {
  const years = alumniData.map(a => a.grad_year).filter(y => y);
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
function renderTopCompaniesTable() {
  const companies = alumniData.map(a => a.company).filter(c => c);
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
function renderTopLocationsTable() {
  const locations = alumniData.map(a => a.location).filter(l => l);
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
