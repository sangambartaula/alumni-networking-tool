/**
 * @jest-environment jsdom
 */

// Create a mock document for testing
describe('Alumni Networking Tool - Frontend Tests', () => {
  let fakeAlumni, createCard, renderProfiles, populateFilters, extractUniqueValues, sortAlumni, filterAlumni;
  
  beforeAll(() => {
    // Load the app.js module after DOM is set up
    const app = require('../public/app.js');
    fakeAlumni = app.fakeAlumni;
    createCard = app.createCard;
    renderProfiles = app.renderProfiles;
    populateFilters = app.populateFilters;
    extractUniqueValues = app.extractUniqueValues;
    sortAlumni = app.sortAlumni;
    filterAlumni = app.filterAlumni;
  });

  beforeEach(() => {
    // Setup DOM
    document.body.innerHTML = `
      <div id="grid"></div>
      <span id="count"></span>
      <div id="locChecks"></div>
      <div id="roleChecks"></div>
      <select id="gradSelect"></select>
      <input id="q" type="text" />
      <select id="sortSelect"></select>
    `;
  });

  describe('Data Utilities', () => {
    test('fakeAlumni contains expected alumni data', () => {
      expect(fakeAlumni).toBeDefined();
      expect(Array.isArray(fakeAlumni)).toBe(true);
      expect(fakeAlumni.length).toBe(5);
      expect(fakeAlumni[0].name).toBe('Sachin Banjade');
    });

    test('extractUniqueValues extracts unique locations', () => {
      const locations = extractUniqueValues(fakeAlumni, 'location');
      expect(locations).toContain('Dallas');
      expect(locations).toContain('Austin');
      expect(locations).toContain('Houston');
    });

    test('extractUniqueValues extracts unique roles', () => {
      const roles = extractUniqueValues(fakeAlumni, 'role');
      expect(roles.length).toBeGreaterThan(0);
      expect(roles).toContain('Software Engineer');
    });
  });

  describe('Sorting Functions', () => {
    test('sortAlumni sorts by name alphabetically', () => {
      const sorted = sortAlumni(fakeAlumni, 'name');
      expect(sorted[0].name).toBe('Abishek Lamichane');
      expect(sorted[sorted.length - 1].name).toBe('Shrish Acharya');
    });

    test('sortAlumni sorts by year descending', () => {
      const sorted = sortAlumni(fakeAlumni, 'year');
      expect(sorted[0].class).toBe(2023);
      expect(sorted[sorted.length - 1].class).toBe(2020);
    });

    test('sortAlumni returns copy when no sort specified', () => {
      const sorted = sortAlumni(fakeAlumni, '');
      expect(sorted.length).toBe(fakeAlumni.length);
      expect(sorted).not.toBe(fakeAlumni); // Should be a copy
    });
  });

  describe('Filtering Functions', () => {
    test('filterAlumni filters by search term', () => {
      const filtered = filterAlumni(fakeAlumni, { term: 'software' });
      expect(filtered.length).toBe(1);
      expect(filtered[0].role).toBe('Software Engineer');
    });

    test('filterAlumni filters by location', () => {
      const filtered = filterAlumni(fakeAlumni, { loc: ['Dallas'] });
      expect(filtered.length).toBe(2);
      expect(filtered.every(a => a.location === 'Dallas')).toBe(true);
    });

    test('filterAlumni filters by graduation year', () => {
      const filtered = filterAlumni(fakeAlumni, { year: '2020' });
      expect(filtered.length).toBe(2);
      expect(filtered.every(a => a.class === 2020)).toBe(true);
    });

    test('filterAlumni filters by multiple criteria', () => {
      const filtered = filterAlumni(fakeAlumni, { 
        loc: ['Dallas'], 
        year: '2020' 
      });
      expect(filtered.length).toBe(2);
      expect(filtered.every(a => a.location === 'Dallas' && a.class === 2020)).toBe(true);
    });

    test('filterAlumni returns all when no filters', () => {
      const filtered = filterAlumni(fakeAlumni, {});
      expect(filtered.length).toBe(fakeAlumni.length);
    });
  });

  describe('Card Creation', () => {
    test('createCard creates article element with correct structure', () => {
      const alumni = fakeAlumni[0];
      const card = createCard(alumni);
      
      expect(card.tagName).toBe('ARTICLE');
      expect(card.className).toBe('card');
      expect(card.getAttribute('data-id')).toBe(String(alumni.id));
      expect(card.innerHTML).toContain(alumni.name);
      expect(card.innerHTML).toContain(alumni.role);
      expect(card.innerHTML).toContain(alumni.company);
    });

    test('createCard includes LinkedIn link', () => {
      const alumni = fakeAlumni[0];
      const card = createCard(alumni);
      const link = card.querySelector('a.btn.link');
      
      expect(link).toBeTruthy();
      expect(link.href).toBe(alumni.linkedin);
    });

    test('createCard includes connect button', () => {
      const alumni = fakeAlumni[0];
      const card = createCard(alumni);
      const connectBtn = card.querySelector('.btn.connect');
      
      expect(connectBtn).toBeTruthy();
      expect(connectBtn.textContent).toBe('Connect');
    });
  });

  describe('Rendering Functions', () => {
    test('renderProfiles renders all profiles to grid', () => {
      const grid = document.getElementById('grid');
      const count = document.getElementById('count');
      
      renderProfiles(fakeAlumni, grid, count);
      
      expect(grid.children.length).toBe(5);
      expect(count.textContent).toBe('(5)');
    });

    test('renderProfiles updates count correctly', () => {
      const grid = document.getElementById('grid');
      const count = document.getElementById('count');
      
      renderProfiles([fakeAlumni[0], fakeAlumni[1]], grid, count);
      
      expect(grid.children.length).toBe(2);
      expect(count.textContent).toBe('(2)');
    });

    test('populateFilters creates location checkboxes', () => {
      populateFilters(fakeAlumni);
      
      const locChecks = document.getElementById('locChecks');
      expect(locChecks.children.length).toBeGreaterThan(0);
      expect(locChecks.innerHTML).toContain('Dallas');
    });

    test('populateFilters creates role checkboxes', () => {
      populateFilters(fakeAlumni);
      
      const roleChecks = document.getElementById('roleChecks');
      expect(roleChecks.children.length).toBeGreaterThan(0);
      expect(roleChecks.innerHTML).toContain('Software Engineer');
    });

    test('populateFilters creates graduation year options', () => {
      populateFilters(fakeAlumni);
      
      const gradSelect = document.getElementById('gradSelect');
      expect(gradSelect.children.length).toBeGreaterThan(1); // Including "All years" option
      expect(gradSelect.innerHTML).toContain('2020');
    });
  });
});
