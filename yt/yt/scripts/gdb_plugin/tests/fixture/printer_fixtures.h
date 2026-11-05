#pragma once

// Builds the typed globals (table-client rows, guid, error, yson) that the
// pretty-printer test (test_printers.py) inspects. Kept in a separate TU from
// the ref-counted retention scenarios in main.cpp.
void SetupGdbPrinterFixtures();
