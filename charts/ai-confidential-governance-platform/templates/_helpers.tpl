{{/*
Expand the name of the chart.
*/}}
{{- define "platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "platform.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Image tag helper — falls back to images.tag if component tag is empty.
*/}}
{{- define "platform.imageTag" -}}
{{- $tag := index . 1 -}}
{{- $global := index . 0 -}}
{{- if $tag -}}
{{- $tag -}}
{{- else -}}
{{- $global.Values.images.tag -}}
{{- end -}}
{{- end }}

{{/*
Full image reference with optional registry prefix.
Usage: include "platform.image" (list . .Values.images.governanceOperator)
*/}}
{{- define "platform.image" -}}
{{- $root := index . 0 -}}
{{- $img := index . 1 -}}
{{- $tag := $img.tag | default $root.Values.images.tag -}}
{{- if $root.Values.global.registry -}}
{{- printf "%s/%s:%s" $root.Values.global.registry $img.repository $tag -}}
{{- else -}}
{{- printf "%s:%s" $img.repository $tag -}}
{{- end -}}
{{- end }}

{{/*
Optional image pull secrets for private registries such as GHCR.
*/}}
{{- define "platform.imagePullSecrets" -}}
{{- with .Values.global.imagePullSecrets }}
imagePullSecrets:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "platform.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/*
Pod security context from values.
*/}}
{{- define "platform.podSecurityContext" -}}
{{- toYaml .Values.podSecurityContext | nindent 8 }}
{{- end }}

{{/*
Container security context from values.
*/}}
{{- define "platform.securityContext" -}}
{{- toYaml .Values.securityContext | nindent 12 }}
{{- end }}
