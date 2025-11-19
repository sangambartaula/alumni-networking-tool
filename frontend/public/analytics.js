// analytics.js
// Analytics dashboard functionality

let alumniData = [];
let charts = {};

// Initialize analytics on page load
document.addEventListener('DOMContentLoaded', () => {
  loadAnalyticsData();
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
  const uniqueLocations = new Set(alumniData.map(a => a.location).filter(l => l)).size;
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
      <div class="job-item">
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

// Extract industry from headline or job title
function extractIndustry(alumni) {
  const headline = (alumni.headline || '').toLowerCase();
  const jobTitle = (alumni.current_job_title || '').toLowerCase();
  const text = `${headline} ${jobTitle}`;
  
  // Simple keyword matching for industries
  if (text.includes('software') || text.includes('developer') || text.includes('engineer')) return 'Software Engineering';
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
          intersect: false
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
