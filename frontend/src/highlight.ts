/**
 * Syntax highlighting for the code shown in the browser. Shiki uses the same grammars and themes as VS Code, so the code
 * looks like it does in the editor (Dark+ and Light+). Only the five languages of the lab are loaded, each on demand.
 */
import { createHighlighterCore, type HighlighterCore } from 'shiki/core'
import { createJavaScriptRegexEngine } from 'shiki/engine/javascript'

/** The language of each worker folder, as Shiki names it. */
export const SHIKI_LANG: Record<string, string> = { python: 'python', node: 'typescript', java: 'java', go: 'go', rust: 'rust' }

const LANG_LOADERS: Record<string, () => Promise<unknown>> = {
  python: () => import('shiki/langs/python.mjs'),
  typescript: () => import('shiki/langs/typescript.mjs'),
  java: () => import('shiki/langs/java.mjs'),
  go: () => import('shiki/langs/go.mjs'),
  rust: () => import('shiki/langs/rust.mjs'),
}

export interface Token { text: string; color?: string; bold?: boolean; italic?: boolean }

let highlighter: Promise<HighlighterCore> | null = null
const loaded = new Set<string>()

function core(): Promise<HighlighterCore> {
  highlighter ??= createHighlighterCore({
    themes: [import('shiki/themes/dark-plus.mjs'), import('shiki/themes/light-plus.mjs')],
    langs: [],
    engine: createJavaScriptRegexEngine(),
  })
  return highlighter
}

/** The code split into lines of coloured pieces. Falls back to plain text if the language cannot be loaded. */
export async function highlight(code: string, lang: string, dark: boolean): Promise<Token[][]> {
  const plain = () => code.split('\n').map((text) => [{ text }])
  const loader = LANG_LOADERS[lang]
  if (!loader) return plain()
  try {
    const h = await core()
    if (!loaded.has(lang)) {
      await h.loadLanguage((await loader()) as never)
      loaded.add(lang)
    }
    const { tokens } = h.codeToTokens(code, { lang, theme: dark ? 'dark-plus' : 'light-plus' })
    return tokens.map((line) =>
      line.map((t) => ({ text: t.content, color: t.color, bold: t.fontStyle ? (t.fontStyle & 2) !== 0 : false, italic: t.fontStyle ? (t.fontStyle & 1) !== 0 : false })),
    )
  } catch {
    return plain()
  }
}
