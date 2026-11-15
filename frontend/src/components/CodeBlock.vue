<script setup lang="ts">
/** Code with line numbers and VS Code colours (Shiki). The colours follow the theme of the page. */
import { ref, watch } from 'vue'
import { highlight, type Token } from '../highlight'
import { theme } from '../theme'

const props = defineProps<{ code: string; title?: string; lang?: string }>()
const lines = ref<Token[][]>(props.code.split('\n').map((text) => [{ text }])) // plain until the colours are ready
const copied = ref(false)

watch(
  () => [props.code, props.lang, theme.value] as const,
  async ([code, lang, current]) => {
    lines.value = props.lang
      ? await highlight(code, lang as string, current === 'dark')
      : code.split('\n').map((text) => [{ text }])
  },
  { immediate: true },
)

async function copy() {
  try {
    await navigator.clipboard.writeText(props.code)
    copied.value = true
    setTimeout(() => (copied.value = false), 1500)
  } catch {
    /* clipboard not available */
  }
}
</script>

<template>
  <div class="code-block" :class="theme">
    <div class="flex items-center justify-between px-3 py-1.5 code-head">
      <span class="lbl mono">{{ title }}</span>
      <button class="btn" style="padding: 2px 10px; font-size: 12px" @click="copy">
        {{ copied ? 'Copied' : 'Copy' }}
      </button>
    </div>
    <div class="overflow-auto" style="max-height: 460px">
      <table class="mono code-table">
        <tbody>
          <tr v-for="(line, i) in lines" :key="i">
            <td class="code-no">{{ i + 1 }}</td>
            <td class="code-line">
              <span
                v-for="(t, k) in line"
                :key="k"
                :style="{
                  color: t.color,
                  fontWeight: t.bold ? 700 : undefined,
                  fontStyle: t.italic ? 'italic' : undefined,
                }"
                >{{ t.text }}</span
              >
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
/* The same colours as the editor: Dark+ and Light+ of VS Code. */
.code-block {
  background: #1e1e1e;
  border: 1px solid #333;
  border-radius: 10px;
  overflow: hidden;
}
.code-block.light {
  background: #ffffff;
  border-color: #d4d4d8;
}
.code-head {
  border-bottom: 1px solid #333;
}
.code-block.light .code-head {
  border-color: #e4e4e7;
}
.code-table {
  font-size: 12px;
  line-height: 1.55;
  border-collapse: collapse;
  width: 100%;
}
.code-no {
  color: #6e7681;
  text-align: right;
  padding: 0 10px;
  user-select: none;
  width: 1%;
  vertical-align: top;
}
.code-line {
  white-space: pre;
  padding-right: 12px;
  color: #d4d4d4;
}
.code-block.light .code-line {
  color: #000000;
}
</style>
