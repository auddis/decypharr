const RAW_VERSIONS = [
	{
		id: 'stable',
		label: 'Stable',
		path: 'stable/',
		description: 'Latest public release.',
	},
	{
		id: 'beta',
		label: 'Beta (Preview)',
		path: 'beta/',
		description: 'Upcoming changes and in-progress features.',
	},
] as const;

export type DocsChannel = (typeof RAW_VERSIONS)[number]['id'];

const DEFAULT_CHANNEL: DocsChannel = 'stable';

const channelFromEnv = (import.meta.env.PUBLIC_DOCS_CHANNEL ?? '').trim().toLowerCase() as
	| DocsChannel
	| '';

const envChannel = RAW_VERSIONS.find((version) => version.id === channelFromEnv)?.id ?? DEFAULT_CHANNEL;

const CURRENT_CHANNEL_BASE = normalizeBasePath(
	import.meta.env.PUBLIC_DOCS_CHANNEL_BASE_PATH || import.meta.env.BASE_URL || '/'
);

const BASE_PREFIX = deriveBasePrefix(CURRENT_CHANNEL_BASE, envChannel);

const versions = RAW_VERSIONS.map((version) => ({
	...version,
	href: withBasePath(BASE_PREFIX, version.path),
}));

const fallbackVersion = versions.find((version) => version.id === envChannel)!;

export type DocsVersion = (typeof versions)[number];

export const getVersions = () => versions;
export const getCurrentVersion = () => fallbackVersion;
export const getDocsChannel = () => fallbackVersion.id;

function withBasePath(base: string, path: string) {
	const cleanPath = path.replace(/^\//, '');

	if (cleanPath.length === 0) {
		return base;
	}

	return `${base}${cleanPath}`.replace(/\/{2,}/g, '/');
}

function normalizeBasePath(value: string) {
	if (!value) {
		return '/';
	}

	let normalized = value.trim();
	if (!normalized.startsWith('/')) {
		normalized = `/${normalized}`;
	}

	if (!normalized.endsWith('/')) {
		normalized = `${normalized}/`;
	}

	return normalized.replace(/\/{2,}/g, '/');
}

function deriveBasePrefix(currentBase: string, channel: DocsChannel) {
	const sanitizedChannel = sanitizeSegment(channel);
	if (!sanitizedChannel) {
		return currentBase;
	}

	const suffix = `${sanitizedChannel}/`;

	if (currentBase.toLowerCase().endsWith(suffix.toLowerCase())) {
		const prefix = currentBase.slice(0, -suffix.length);
		return prefix.length === 0 ? '/' : prefix;
	}

	return currentBase;
}

function sanitizeSegment(value: string) {
	if (!value) {
		return '';
	}

	return value.replace(/^\/+/, '').replace(/\/+$/, '');
}
