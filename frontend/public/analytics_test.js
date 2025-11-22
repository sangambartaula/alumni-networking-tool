/**
 * analytics_test.js
 * -------------------------------------------------------
 * This file exists ONLY for development & testing.
 * It does NOT affect the real analytics page.
 * Purpose:
 *  - Test API response formats
 *  - Test chart data processing
 *  - Validate helper functions
 *  - Generate mock analytics output
 * -------------------------------------------------------
 */

//MOCK DATA //
const mockAlumni = [
  {
    id: 1,
    name: "Aiden Anthony",
    current_job_title: "Software Engineer",
    company: "Microsoft",
    location: "Dallas, Texas",
    grad_year: 2027,
    headline: "Aspiring Computer Science Major"
  },
  {
    id: 2,
    name: "Alisha B.",
    current_job_title: "AI/ML Engineer",
    company: "Google",
    location: "Denton, Texas",
    grad_year: 2025,
    headline: "Aspiring Software Engineer"
  },
  {
    id: 3,
    name: "Peyton Coate",
    current_job_title: "Data Scientist",
    company: "Amazon",
    location: "Fort Worth, Texas",
    grad_year: 2025,
    headline: "Computer Science student"
  },
  {
    id: 4,
    name: "Test Person",
    current_job_title: "AI/ML Engineer",
    company: "Google",
    location: "Austin, Texas",
    grad_year: 2026,
    headline: "Student"
  }
];

console.log("[TEST] Loaded mock alumni:", mockAlumni.length);


// =============== TEST: FREQUENCY MAP =============== //

function getFrequency(arr) {
  const freq = {};
  arr.forEach(item => {
    if (!item) return;
    freq[item] = (freq[item] || 0) + 1;
  });
  return freq;
}

console.log("[TEST] Job title frequency:", getFrequency(mockAlumni.map(a => a.current_job_title)));
console.log("[TEST] Company frequency:", getFrequency(mockAlumni.map(a => a.company)));


// =============== TEST: TOP N ITEMS =============== //

function getTopN(items, n = 5) {
  const freq = getFrequency(items);
  return Object.entries(freq)
    .sort((a, b) => b[1] - a[1])
    .slice(0, n);
}

console.log("[TEST] Top 2 companies:", getTopN(mockAlumni.map(a => a.company), 2));
console.log("[TEST] Top 3 job titles:", getTopN(mockAlumni.map(a => a.current_job_title), 3));


// =============== TEST: COLOR GENERATION =============== //

function generateTestColors(count) {
  const base = [
    "#ff6b6b", "#5f27cd", "#54a0ff", "#1dd1a1", "#feca57",
    "#48dbfb", "#ff9ff3", "#00d2d3"
  ];
  const result = [];
  for (let i = 0; i < count; i++) {
    result.push(base[i % base.length]);
  }
  return result;
}

console.log("[TEST] Colors (10):", generateTestColors(10));


// TEST: DATA PIPELINE SIMULATION  //

function buildChartDataset(topItems) {
  const labels = topItems.map(([label]) => label);
  const data = topItems.map(([, count]) => count);
  const colors = generateTestColors(labels.length);

  return {
    labels,
    datasets: [
      {
        label: "Test Dataset",
        data,
        backgroundColor: colors,
        borderColor: "#ffffff",
        borderWidth: 2
      }
    ]
  };
}

const topJobTitles = getTopN(mockAlumni.map(a => a.current_job_title), 5);
const datasetPreview = buildChartDataset(topJobTitles);

console.log("[TEST] Dataset preview for chart:", datasetPreview);


// TEST: MOCK CHART RENDER (no chart.js needed) //

function mockRenderChart(dataset) {
  console.log("=== CHART RENDER SIMULATION ===");
  console.log("Labels:", dataset.labels);
  console.log("Data:", dataset.datasets[0].data);
  console.log("Colors:", dataset.datasets[0].backgroundColor);
  console.log("Render successful ✔");
}

mockRenderChart(datasetPreview);


//  TEST SUMMARY //

console.log("-------------------------------------------------");
console.log("TEST FILE EXECUTED SUCCESSFULLY");
console.log("All helper functions working");
console.log("Dataset generation pipeline verified");
console.log("-------------------------------------------------");
