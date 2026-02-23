/**
 * profile_modal_test.js
 * -------------------------------------------------------
 * Automated tests for the ProfileDetailModal component.
 * Runs in-browser and logs results to the console.
 * -------------------------------------------------------
 */

(function () {
    'use strict';

    let passed = 0;
    let failed = 0;

    function assert(condition, testName) {
        if (condition) {
            console.log(`[PASS] ${testName}`);
            passed++;
        } else {
            console.error(`[FAIL] ${testName}`);
            failed++;
        }
    }

    function runTests() {
        console.log('========================================');
        console.log('ProfileDetailModal Tests');
        console.log('========================================');

        // Create a temporary modal instance for testing
        const modal = new ProfileDetailModal();

        // ---- Test 1: Conditional rendering — only present educations ----
        const dataFull = {
            id: 1,
            name: 'Test User',
            school: 'University of North Texas',
            degree: 'B.S.',
            major: 'Computer Science',
            grad_year: 2025,
            school_start_date: '2021',
            school2: 'MIT',
            degree2: 'M.S.',
            major2: 'AI',
            school3: null,
            degree3: null,
            major3: null,
            current_job_title: 'Software Engineer',
            company: 'Google',
            job_start_date: '2025',
            job_end_date: 'Present',
            exp2_title: 'Intern',
            exp2_company: 'Microsoft',
            exp2_dates: 'Jun 2024 - Aug 2024',
            exp3_title: null,
            exp3_company: null,
            exp3_dates: null,
            headline: 'Passionate engineer building great things',
            updated_at: '2025-01-15T10:00:00'
        };

        modal.open(dataFull, null);
        const body = document.getElementById('profileDetailBody');

        // Check Education 1 and 2 rendered, Education 3 not rendered
        const labels = body.querySelectorAll('.profile-detail-label');
        const labelTexts = Array.from(labels).map(l => l.textContent.trim());

        assert(labelTexts.includes('Education'), 'Education 1 label rendered');
        assert(labelTexts.includes('Education 2'), 'Education 2 label rendered');
        assert(!labelTexts.includes('Education 3'), 'Education 3 NOT rendered when null');

        // Check Experience 1 and 2 rendered, Experience 3 not rendered
        assert(labelTexts.includes('Experience'), 'Experience 1 label rendered');
        assert(labelTexts.includes('Experience 2'), 'Experience 2 label rendered');
        assert(!labelTexts.includes('Experience 3'), 'Experience 3 NOT rendered when null');

        modal.close();

        // ---- Test 2: Experience formatting with dates ----
        const values = Array.from(body.querySelectorAll('.profile-detail-value')).map(v => v.textContent.trim());

        // Exp 1 should have company - title (start - end)
        const exp1Expected = 'Google - Software Engineer (2025 - Present)';
        assert(values.some(v => v === exp1Expected), `Experience 1 formatted as "${exp1Expected}"`);

        // Exp 2 should have dates from exp2_dates
        const exp2Expected = 'Microsoft - Intern (Jun 2024 - Aug 2024)';
        assert(values.some(v => v === exp2Expected), `Experience 2 formatted as "${exp2Expected}"`);

        // ---- Test 3: Experience without dates omits parentheses ----
        const dataNoDates = {
            id: 2, name: 'No Dates Person',
            current_job_title: 'Designer',
            company: 'Figma',
            job_start_date: null,
            job_end_date: null,
            headline: 'UX Designer',
            school: 'UNT'
        };

        modal.open(dataNoDates, null);
        const valuesNoDates = Array.from(body.querySelectorAll('.profile-detail-value')).map(v => v.textContent.trim());
        const expNoDatesExpected = 'Figma - Designer';
        assert(
            valuesNoDates.some(v => v === expNoDatesExpected),
            `Experience without dates omits parentheses: "${expNoDatesExpected}"`
        );
        modal.close();

        // ---- Test 4: Headline clamp + Show more/less ----
        const longHeadline = 'A'.repeat(500); // Very long headline
        const dataLongHL = {
            id: 3, name: 'Long Headline Person',
            headline: longHeadline,
            school: 'UNT', current_job_title: 'Dev', company: 'Co'
        };

        modal.open(dataLongHL, null);
        const hlEl = body.querySelector('.profile-detail-headline');
        assert(hlEl !== null, 'Headline element exists');
        assert(!hlEl.classList.contains('expanded'), 'Headline starts clamped');

        // Check toggle button appears (may need requestAnimationFrame to resolve)
        setTimeout(() => {
            const toggleBtn = body.querySelector('.profile-detail-toggle');
            assert(toggleBtn !== null, 'Show more toggle button exists');

            if (toggleBtn) {
                // Click to expand
                toggleBtn.click();
                assert(hlEl.classList.contains('expanded'), 'Headline expanded after "Show more" click');
                assert(toggleBtn.textContent === 'Show less', 'Toggle text changed to "Show less"');

                // Click to collapse
                toggleBtn.click();
                assert(!hlEl.classList.contains('expanded'), 'Headline collapsed after "Show less" click');
                assert(toggleBtn.textContent === 'Show more', 'Toggle text changed back to "Show more"');
            }

            modal.close();

            // ---- Test 5: Modal open/close with ESC ----
            const overlay = document.getElementById('profileDetailOverlay');
            modal.open({ id: 99, name: 'ESC Test', school: 'UNT', company: 'X', current_job_title: 'Y' }, null);
            assert(overlay.classList.contains('show'), 'Modal opens with show class');

            // Simulate ESC
            const escEvent = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true });
            document.dispatchEvent(escEvent);

            assert(!overlay.classList.contains('show'), 'Modal closes on ESC key');

            // ---- Test 6: Missing fields — no crash, no "undefined" ----
            const dataMinimal = { id: 100, name: null };
            modal.open(dataMinimal, null);
            const bodyHtml = body.innerHTML;
            assert(!bodyHtml.includes('undefined'), 'No "undefined" text rendered with minimal data');
            assert(!bodyHtml.includes('null'), 'No "null" text rendered with minimal data');
            modal.close();

            // ---- Test 7: Missing experience/education arrays (empty strings) ----
            const dataEmpty = {
                id: 101, name: '', school: '', degree: '', major: '',
                company: '', current_job_title: '', headline: '',
                school2: '', degree2: '', school3: '', degree3: '',
                exp2_title: '', exp2_company: '', exp3_title: '', exp3_company: ''
            };
            modal.open(dataEmpty, null);
            const bodyHtmlEmpty = body.innerHTML;
            assert(!bodyHtmlEmpty.includes('undefined'), 'No "undefined" with empty string data');
            modal.close();

            // ---- Test 8: Regression — renderFilteredAlumni table structure ----
            if (typeof renderFilteredAlumni === 'function') {
                // Verify the function exists and table structure is intact
                const table = document.getElementById('filteredAlumniTable');
                if (table) {
                    const headers = Array.from(table.querySelectorAll('th')).map(th => th.textContent.trim());
                    assert(headers.includes('Name'), 'Filtered alumni table has Name column');
                    assert(headers.includes('Job Title'), 'Filtered alumni table has Job Title column');
                    assert(headers.includes('Actions'), 'Filtered alumni table has Actions column');
                    assert(headers.length === 6, 'Filtered alumni table has 6 columns (no layout change)');
                } else {
                    console.log('[SKIP] filteredAlumniTable not found on this page');
                }
            } else {
                console.log('[SKIP] renderFilteredAlumni not available on this page');
            }

            // Summary
            console.log('========================================');
            console.log(`RESULTS: ${passed} passed, ${failed} failed`);
            console.log('========================================');
        }, 100); // Small delay for RAF to process headline overflow
    }

    // Run after DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', runTests);
    } else {
        // Small delay to let profile_modal.js initialize first
        setTimeout(runTests, 200);
    }
})();
