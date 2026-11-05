#pragma once

// Builds the tcmalloc page-map fixture: a single large (multi-MB) allocation that
// tcmalloc serves as one span, so the page-map walk must snap an interior pointer
// back to the span start.
void SetupGdbTcmallocFixtures();
