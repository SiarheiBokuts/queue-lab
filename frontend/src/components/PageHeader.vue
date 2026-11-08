<script setup lang="ts">
/** The top of every page: where you are (breadcrumbs), what the page is, and what it is for. */
defineProps<{ title: string; subtitle?: string; crumbs?: { label: string; to?: string }[] }>()
</script>

<template>
  <div>
    <div v-if="crumbs?.length" class="lbl mb-1">
      <template v-for="(c, i) in crumbs" :key="i">
        <router-link v-if="c.to" :to="c.to" class="hover:underline">{{ c.label }}</router-link>
        <span v-else>{{ c.label }}</span>
        <span v-if="i < crumbs.length - 1" style="margin: 0 6px">/</span>
      </template>
    </div>
    <h1>{{ title }}</h1>
    <p v-if="subtitle" class="lbl mt-1" style="max-width: 780px; line-height: 1.5">{{ subtitle }}</p>
  </div>
</template>