//go:build windows

package computestorage

import (
	"fmt"

	"golang.org/x/sys/windows"

	"github.com/Microsoft/go-winio/internal/interop"
)

//go:generate go run github.com/Microsoft/go-winio/tools/mkwinsyscall -output zsyscall_windows.go computestorage.go

// https://learn.microsoft.com/en-us/virtualization/api/hcs/reference/hcsformatwritablelayervhd
//
//sys hcsFormatWritableLayerVhd(handle windows.Handle) (hr error) = computestorage.HcsFormatWritableLayerVhd?

// FormatWritableLayerVHD formats a virtual disk for use as a writable container layer.
//
// If the VHD is not mounted it will be temporarily mounted.
//
// NOTE: This API had a breaking change in the operating system after Windows Server 2019.
// On ws2019 the API expects to get passed a file handle from CreateFile for the vhd that
// the caller wants to format. On > ws2019, its expected that the caller passes a vhd handle
// that can be obtained from the virtdisk APIs.
func FormatWritableLayerVHD(vhdHandle windows.Handle) (err error) {
	err = hcsFormatWritableLayerVhd(vhdHandle)
	if err != nil {
		return fmt.Errorf("failed to format writable layer vhd: %w", err)
	}
	return nil
}

// https://learn.microsoft.com/en-us/virtualization/api/hcs/reference/hcsgetlayervhdmountpath
//
//sys hcsGetLayerVhdMountPath(vhdHandle windows.Handle, mountPath **uint16) (hr error) = computestorage.HcsGetLayerVhdMountPath?

// GetLayerVHDMountPath returns the volume path for a virtual disk of a writable container layer.
func GetLayerVHDMountPath(vhdHandle windows.Handle) (path string, err error) {
	var mountPath *uint16
	err = hcsGetLayerVhdMountPath(vhdHandle, &mountPath)
	if err != nil {
		return "", fmt.Errorf("failed to get vhd mount path: %w", err)
	}
	path = interop.ConvertAndFreeCoTaskMemString(mountPath)
	return path, nil
}
