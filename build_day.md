# Pizza Order Voice Agent

## Your Task

Build a voice agent that places an outbound phone call and orders pizza for delivery. The agent navigates an automated phone menu (IVR), waits on hold, then speaks with a human employee to place the order. Use Claude Code and the internet. Ask Eshan for any API keys or services you need.

## Call Data

Your agent receives a JSON payload with the order details. Here's an example:

```json
{
  "customer_name": "Jordan Mitchell",
  "phone_number": "5125550147",
  "delivery_address": "4821 Elm Street, Apt 3B, Austin, TX 78745",
  "pizza": {
    "size": "large",
    "crust": "thin",
    "toppings": ["pepperoni", "mushroom", "green pepper"],
    "acceptable_topping_subs": ["sausage", "bacon", "onion", "spinach", "jalapeño"],
    "no_go_toppings": ["olives", "anchovies", "pineapple"]
  },
  "side": {
    "first_choice": "buffalo wings, 12 count",
    "backup_options": ["garlic bread", "breadsticks", "mozzarella sticks"],
    "if_all_unavailable": "skip"
  },
  "drink": {
    "first_choice": "2L Coke",
    "alternatives": ["2L Pepsi", "2L Sprite"],
    "skip_if_over_budget": true
  },
  "budget_max": 45.00,
  "special_instructions": "Ring doorbell, don't knock (baby sleeping)"
}
```

**Important:** These are example values. During testing, I will use different names, addresses, orders, and budgets. Your agent must work with any valid data — do not hardcode these specific values. Treat every field as a variable.

**Field definitions:**

| Field | Type | Description |
|---|---|---|
| `customer_name` | string | Name for the order |
| `phone_number` | string | 10-digit callback number |
| `delivery_address` | string | Full delivery address including zip code |
| `pizza.size` | string | Pizza size (e.g. "large", "medium") |
| `pizza.crust` | string | Crust type (e.g. "thin", "hand-tossed") |
| `pizza.toppings` | list | Desired toppings, in order of preference |
| `pizza.acceptable_topping_subs` | list | If a topping is unavailable, accept any of these as a replacement. Only use these — don't accept random suggestions. |
| `pizza.no_go_toppings` | list | Never accept these toppings, even if the employee offers them. |
| `pizza` (overall) | object | The core item. Must be ordered. |
| `side.first_choice` | string | Side to order first |
| `side.backup_options` | list | If first choice unavailable, try these in order |
| `side.if_all_unavailable` | string | What to do if all side options are gone. `"skip"` means don't order a side. |
| `drink.first_choice` | string | Drink to order first |
| `drink.alternatives` | list | If first choice unavailable, try these in order |
| `drink.skip_if_over_budget` | bool | If true, don't order the drink if it would push the total over `budget_max` |
| `budget_max` | number | Maximum allowed total in dollars |
| `special_instructions` | string | Delivery notes to give the employee before hanging up |

## The IVR (Automated Phone Menu)

When the call connects, your agent hits an IVR — a legacy automated phone system. Here are the exact prompts it will hear, in order:

### Prompt 1: Welcome + Menu

```
"Thank you for calling. Press 1 for delivery. Press 2 for carryout.
Press 3 to hear store hours."
```

Your agent needs delivery — press 1.

### Prompt 2: Name

```
"Please say the name for the order."
```

Your agent says the customer's name.

### Prompt 3: Callback Number

```
"Please enter your 10-digit callback number."
```

Your agent enters the phone number.

### Prompt 4: Delivery Zip Code

```
"Please say your delivery zip code."
```

Your agent says the zip code from the delivery address.

### Prompt 5: Confirmation

```
"I heard [name], [number], zip code [zip]. Is that correct?
Say 'yes' to confirm. Say 'no' to start over."
```

Your agent confirms or re-enters.

### Prompt 6: Transfer

```
"Got it. Please hold while we connect you to a team member."
```

Hold music or silence follows. A human will eventually pick up.

### IVR Behavior

This is a **legacy IVR system**. It does NOT understand natural language. Each prompt tells you exactly how to respond — "press", "enter", and "say" each mean something specific. The IVR only accepts exactly what it asked for, in exactly the format it asked for.

**What works:**
- Responding in exactly the format the prompt asked for
- Saying exactly the value requested, with no extra words

**What fails** (IVR says "I didn't understand that, please try again"):
- `"My name is Jordan Mitchell"` — prefix confuses it, just say the name
- `"Yes, that's correct"` — only the word "yes" is recognized
- Responding in a different format than requested
- Any extra words beyond what was asked for

If the IVR gets a response it doesn't understand, it repeats the current prompt up to 3 times, then says "I'm sorry, I'm having trouble understanding you. Please call back later" and hangs up.

## Hold

After the IVR transfers you, you'll hear hold music or silence. Could be a few seconds or a few minutes.

Your agent should:
- Not say anything while on hold
- Detect when a human picks up (a real person speaking, not a recording)
- Once a human greets you, start the conversation

## The Conversation

Once a human employee picks up, your agent places the order. Humans usually introduce themseleves when they pick up. 

Order the pizza, side, and drink according to the call data. Get the individual price for each item. Handle substitutions according to the rules in the data — accept toppings from `acceptable_topping_subs`, reject anything in `no_go_toppings`, try side `backup_options` in order. Get the total, delivery time, and order/confirmation number. Deliver the `special_instructions` before hanging up.

The employee is a real person. They'll pause while typing — they're not asking you a question. They might ask you to repeat info you already gave. They might give vague answers ("about thirty bucks", "35-40 minutes") — push for exact numbers. They might interrupt you. Your agent needs to handle all of this realistically — the way a normal person would on a phone call.

### Hangup Conditions

Your agent must end every call with one of these outcomes:

| Outcome | When |
|---|---|
| `completed` | Pizza + side confirmed (or side skipped per data rules), prices collected, total + delivery time + order number received, special instructions delivered. Drink is optional — skip if over budget. |
| `nothing_available` | The pizza itself can't be ordered (core item unavailable). Hang up. |
| `over_budget` | Pizza + side already exceeds `budget_max` — don't order the drink, apologize, hang up. |
| `detected_as_bot` | Employee suspects a bot or can't understand the agent — hang up immediately, no arguing. |

## Output

When the call ends, your agent should print a structured JSON result:

```json
{
  "outcome": "completed",
  "pizza": {
    "description": "large hand-tossed with pepperoni, onion, green pepper",
    "substitutions": {"mushroom": "onion", "thin crust": "hand-tossed"},
    "price": 18.50
  },
  "side": {
    "description": "garlic bread",
    "original": "buffalo wings, 12 count",
    "price": 6.99
  },
  "drink": {
    "description": "2L Coke",
    "price": 3.49
  },
  "total": 28.98,
  "delivery_time": "35 minutes",
  "order_number": "4412",
  "special_instructions_delivered": true
}
```

For non-completed outcomes, include whatever was collected before the call ended. If the drink was skipped due to budget, set `"drink": null` with a note explaining why.

## How to Test

1. Have your agent call **your own phone number**
2. You pick up and play the employee
3. Listen to what the agent says — fix issues — repeat

You'll go through many iterations. This is expected.

### Test Script (You Play the Employee)

Use this script to test the happy path with one topping substitution and per-item pricing.

```
[YOUR PHONE RINGS — PICK UP]

=== IVR (read this in a flat, robotic voice) ===

"Thank you for calling.
 Press 1 for delivery. Press 2 for carryout.
 Press 3 to hear store hours."

[WAIT FOR AGENT RESPONSE]

"Please say the name for the order."

[WAIT — agent should say just the customer name, nothing else]

"Please enter your 10-digit callback number."

[WAIT FOR AGENT RESPONSE]

"Please say your delivery zip code."

[WAIT — agent should say just the zip code]

"I heard [repeat what you heard]. Is that correct?
 Say 'yes' to confirm. Say 'no' to start over."

[WAIT — agent should say just "yes"]

"Got it. Please hold while we connect you to a team member."

[PAUSE 5-10 SECONDS — THIS IS THE HOLD PERIOD]

=== HUMAN (speak naturally now) ===

"Hey thanks for calling, what can I get started for you?"

[Agent should say it's for delivery and start giving info]

"Name for the order?"

[Agent gives name]

"And the phone number?"

[Agent gives number]

"Delivery address?"

[Agent gives address]

"Cool, what are you looking to order?"

[Agent orders the pizza — size, crust, toppings]

"Okay..." [pause 3 seconds, say nothing — agent should wait]

"So we're actually out of mushroom today. How about olives on that instead?"

[Agent should DECLINE olives (no_go_toppings) and ask for alternatives]

"We could do onion?"

[Agent should ACCEPT onion (acceptable_topping_subs)]

"Got it." [pause 3 seconds] "That pizza is $18.50."

[Agent asks about the side]

"No wings today actually. We do have garlic bread though."

[Agent should accept garlic bread (first backup_option)]

"That's $6.99." [pause 2 seconds] "So $25.49 so far."

[Agent asks about the drink]

"2-liter Coke, that's $3.49."

"Anything else?"

[Agent should say no]

"Alright your total is $28.98. Should be about 35 minute
 Order number is 4412."

[Agent should deliver special instructions, then say thanks and hang up]
```
