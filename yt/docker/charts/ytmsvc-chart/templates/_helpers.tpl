{{/*
Expand the name of the chart.
*/}}
{{- define "ytmsvc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "ytmsvc.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create a default fully qualified app name for cronjobs.
*/}}
{{- define "ytmsvc.cronjob.fullname" -}}
{{- $suffix := .suffix -}}
{{- $root := .root -}}
{{- if $root.Values.fullnameOverride }}
{{- printf "%s-%s" $root.Values.fullnameOverride $suffix | trunc 52 | trimSuffix "-" }}
{{- else }}
{{- $name := default $root.Chart.Name $root.Values.nameOverride }}
{{- if contains $name $root.Release.Name }}
{{- printf "%s-%s" $root.Release.Name $suffix | trunc 52 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s-%s" $root.Release.Name $name $suffix | trunc 52 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ytmsvc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ytmsvc.commonLabels" -}}
helm.sh/chart: {{ include "ytmsvc.chart" . }}
{{ include "ytmsvc.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ytmsvc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ytmsvc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "ytmsvc.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ytmsvc.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Image configuration helper
*/}}
{{- define "ytmsvc.image" -}}
{{- $image := .image | default dict -}}
{{- $repository := $image.repository | default .root.Values.image.repository -}}
{{- $tag := $image.tag | default .root.Values.image.tag | default .root.Chart.AppVersion -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}

{{/*
Validate required values
*/}}
{{- define "ytmsvc.validateValues" -}}
{{- if not .Values.cluster.name }}
{{- fail "cluster.name is required" }}
{{- end }}
{{- if not .Values.cluster.proxy }}
{{- fail "cluster.proxy is required" }}
{{- end }}
{{- end }}

{{- include "ytmsvc.validateValues" . }}

{{/*
Bulk ACL checker base URL
*/}}
{{- define "ytmsvc.bulkAclCheckerBaseUrl" -}}
{{- .Values.microservices.resourceUsage.api.config.bulkAclCheckerBaseUrl | default (printf "http://%s-bulk-acl-checker-service:%v" (include "ytmsvc.fullname" .) .Values.microservices.bulkAclChecker.api.service.port) -}}
{{- end -}}

{{/*
Resource Usage logs directory
*/}}
{{- define "ytmsvc.resourceUsage.logsDir" -}}
{{- .Values.microservices.resourceUsage.api.config.logsDir -}}
{{- end -}}
