<script setup lang="ts">
/** Step 5: how many messages, how big, how fast, how compressed, from how many users. */
import { fmtMiB } from '../../compression'
import { groupDigits } from '../../format'
import { useRunForm } from '../../composables/useRunForm'
import Hint from '../Hint.vue'
import NumberInput from '../NumberInput.vue'

const { form, duration, net, rateMibS } = useRunForm()

/** Ready-made amounts as buttons; the number field next to them takes any other value. */
const MESSAGE_STEPS = [1_000, 10_000, 100_000, 1_000_000, 10_000_000]
const RATE_STEPS = [0, 1_000, 10_000, 50_000, 100_000]
const short = (n: number) => (n === 0 ? 'Max' : n >= 1_000_000 ? n / 1_000_000 + 'M' : n / 1_000 + 'k')
</script>

<template>
  <div class="card">
    <h2 class="mb-3">5. The load</h2>
    <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(220px, 1fr))">
      <label class="field"
        >Messages in total
        <Hint text="Any number. A very big one is allowed too: the box under Check and start says what to expect, for example when the run would take longer than the 30 minutes after which a run is stopped." />
        <NumberInput v-model="form.messages" :min="1" />
        <span class="chips"
          ><button
            v-for="n in MESSAGE_STEPS"
            :key="n"
            type="button"
            class="chip"
            :class="{ on: form.messages === n }"
            @click.prevent="form.messages = n"
          >
            {{ short(n) }}
          </button></span
        >
        <span class="lbl">= {{ groupDigits(form.messages || 0) }} messages</span>
      </label>
      <label class="field"
        >Message size (bytes)
        <Hint text="Includes a small header of about 30 bytes. Kafka's default limit is about 1 MB per message." />
        <NumberInput v-model="form.message_size" :min="64" />
      </label>
      <label class="field"
        >Send speed (messages per second)
        <Hint
          text="The target for all senders together. 0 means as fast as possible. Set a number to hold a steady load, for example 20000 per second."
        />
        <NumberInput v-model="form.rate" :min="0" />
        <span class="chips"
          ><button
            v-for="n in RATE_STEPS"
            :key="n"
            type="button"
            class="chip"
            :class="{ on: form.rate === n }"
            @click.prevent="form.rate = n"
          >
            {{ short(n) }}
          </button></span
        >
        <span class="lbl">{{
          form.rate ? groupDigits(form.rate) + ' messages per second' : 'as fast as possible'
        }}</span>
      </label>
      <label class="field" v-if="form.rate > 0"
        >...or for how long (seconds)
        <Hint text="Sets the number of messages to rate x seconds." />
        <input class="input" type="number" min="1" v-model.number="duration" />
      </label>
      <label class="field"
        >Compression
        <Hint
          text="Really compresses every batch inside the senders before it is sent, with the same codec in every language. It saves network and disk and costs some CPU. It only helps with data that repeats. The tile below shows an approximate result, the report the real one."
        />
        <select class="input" v-model="form.compression">
          <option value="none">None</option>
          <option value="lz4">lz4 (fast)</option>
          <option value="snappy">snappy</option>
          <option value="gzip">gzip (small, slower)</option>
          <option value="zstd">zstd (balanced)</option>
        </select>
      </label>
      <label class="field"
        >Different users
        <Hint
          text="Every message belongs to a user (its key), like a payment of a customer. A user always lands in the same partition, so Kafka keeps that user's messages in order. Messages of one user are always handled one after another, so more users means more messages can be handled side by side: the number of users limits how many run at the same time."
        />
        <NumberInput v-model="form.users" :min="1" />
      </label>
      <label class="field"
        >Message content
        <Hint
          text="Compressible: repeated filler, shrinks a lot. Random: like encrypted data, does not shrink. Use random to see what compression really costs."
        />
        <select class="input" v-model="form.payload">
          <option value="compressible">Compressible filler</option>
          <option value="random">Random bytes</option>
        </select>
      </label>
    </div>

    <div class="grid gap-2.5 mt-4" style="grid-template-columns: repeat(auto-fit, minmax(200px, 1fr))">
      <div class="tile">
        <div class="lbl">Data in total</div>
        <div class="val">{{ fmtMiB(net.beforeMiB) }}</div>
        <div class="lbl">messages x size, before compression</div>
      </div>
      <div class="tile" :style="{ borderLeft: '3px solid ' + (net.ratio > 1 ? '#10b981' : '#3f3f46') }">
        <div class="lbl">On the network (estimate)</div>
        <div class="val">≈ {{ fmtMiB(net.afterMiB) }}</div>
        <div class="lbl" v-if="form.compression === 'none'">no compression: the data goes as it is</div>
        <div class="lbl" v-else-if="net.ratio === 1">
          random data does not shrink, so {{ form.compression }} saves nothing
        </div>
        <div class="lbl" v-else>
          ≈ {{ Math.round(net.ratio) }}× smaller with {{ form.compression }}, about {{ net.savedPct }}% less. The report
          shows the real numbers.
        </div>
      </div>
      <div class="tile" v-if="form.rate > 0">
        <div class="lbl">Network needed while sending</div>
        <div class="val">{{ rateMibS.toFixed(1) }} MiB/s</div>
        <div class="lbl">≈ {{ (rateMibS * 8).toFixed(0) }} Mbit/s, before compression</div>
      </div>
      <div class="tile" v-if="form.rate > 0">
        <div class="lbl">Sending takes about</div>
        <div class="val">{{ duration }} s</div>
      </div>
      <div class="tile">
        <div class="lbl">Each sender</div>
        <div class="val">
          {{ form.rate > 0 ? Math.round(form.rate / form.producer.count).toLocaleString() + ' msg/s' : 'full speed' }}
        </div>
      </div>
    </div>
    <div class="lbl mt-2">
      The report afterwards shows how much network the data needed and how much was really sent.
    </div>
  </div>
</template>
